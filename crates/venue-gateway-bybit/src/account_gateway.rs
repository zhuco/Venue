use std::{collections::BTreeMap, str::FromStr};

use serde::Deserialize;
use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{
    Amount, Asset, ExecutionCommand, FieldState, Fill, MarketReduceCommand, NativeOrderFamily,
    OrderCommand, OrderSide, OrderState, PositionSide, Price, Symbol,
};
use venue_execution::{
    AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
    AccountInstrumentIdentity, AccountLimitNormalizationIntent, AccountPhysicalGateway,
    AccountRecoveryOutcome, AccountRecoveryReport, AccountRecoveryRequest, AccountRiskEvidence,
    SignedAccountBalance, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact, SignedUnknownResult,
};
use venue_gateway_api::GatewayBinding;

use crate::private::diagnose_position_page;
use crate::transport::unix_ms;
use crate::{
    BybitAccountIdentity, BybitCancelIntent, BybitClosedOrderReadback, BybitCredentials,
    BybitGatewayBinding, BybitHistoryWindow, BybitHttpTransport, BybitLinearInstrumentRules,
    BybitOpenOrderPage, BybitOrderEvidencePage, BybitOrderKind, BybitOrderLookup, BybitPlaceIntent,
    BybitPositionReadback, BybitPreparedPrivateRequest, BybitPrivateSource, BybitTimeInForce,
    BybitTransportError, BybitTransportLimits, parse_account_identity, parse_api_key_evidence,
    parse_linear_instrument, parse_open_order_page, parse_order_history_page, parse_position_page,
    parse_rest_bbo, prepare_cancel_request, prepare_place_request, prepare_private_request,
    settle_order_ack,
};

const EXACT_READBACK_MAX_PAGES: u32 = 32;
const HISTORY_WINDOW_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const FILLS_CURSOR_OVERLAP_MS: u64 = 60_000;
const LIMIT_BBO_MAX_AGE_MS: u64 = 1_000;

/// Production Bybit adapter for the lightweight account host. It can only POST while consuming
/// the host's linear permit; callers cannot obtain or clone that permit from this crate.
pub struct BybitAccountGateway {
    runtime: Runtime,
    binding: BybitGatewayBinding,
    credentials: BybitCredentials,
    transport: BybitHttpTransport,
    identity: BybitAccountIdentity,
    positions: BybitPositionReadback,
    rules: BybitLinearInstrumentRules,
    symbol_catalog: BTreeMap<Symbol, BybitSymbolScope>,
    next_attempt_id: u64,
}

struct BybitSymbolScope {
    binding: BybitGatewayBinding,
    transport: BybitHttpTransport,
    rules: BybitLinearInstrumentRules,
}

impl BybitAccountGateway {
    /// Performs public-rule and signed account/permission/position preflight against production.
    /// Construction does not issue any mutation.
    pub fn connect_from_environment(
        binding: GatewayBinding,
        limits: BybitTransportLimits,
    ) -> Result<Self, BybitAccountGatewayError> {
        let credentials = BybitCredentials::from_environment()
            .map_err(|_| BybitAccountGatewayError::Credentials)?;
        Self::connect(binding, credentials, limits)
    }

    fn connect(
        binding: GatewayBinding,
        credentials: BybitCredentials,
        limits: BybitTransportLimits,
    ) -> Result<Self, BybitAccountGatewayError> {
        let binding =
            BybitGatewayBinding::new(binding).map_err(|_| BybitAccountGatewayError::Binding)?;
        let generation = unix_ms().map_err(BybitAccountGatewayError::Transport)?;
        let transport = BybitHttpTransport::new(&binding, generation, limits)
            .map_err(BybitAccountGatewayError::Transport)?;
        Self::connect_with_transport(binding, credentials, transport, generation)
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn connect_with_endpoint(
        binding: GatewayBinding,
        credentials: BybitCredentials,
        limits: BybitTransportLimits,
        endpoint: String,
        generation: u64,
    ) -> Result<Self, BybitAccountGatewayError> {
        let binding =
            BybitGatewayBinding::new(binding).map_err(|_| BybitAccountGatewayError::Binding)?;
        let transport = BybitHttpTransport::with_endpoint(&binding, generation, endpoint, limits)
            .map_err(BybitAccountGatewayError::Transport)?;
        Self::connect_with_transport(binding, credentials, transport, generation)
    }

    fn connect_with_transport(
        binding: BybitGatewayBinding,
        credentials: BybitCredentials,
        transport: BybitHttpTransport,
        generation: u64,
    ) -> Result<Self, BybitAccountGatewayError> {
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| BybitAccountGatewayError::Runtime)?;
        let (rules, identity, positions) =
            runtime.block_on(bootstrap(&binding, &credentials, &transport, generation, 1))?;
        let symbol_catalog = BTreeMap::from([(
            binding.gateway_binding().symbol.clone(),
            BybitSymbolScope {
                binding: binding.clone(),
                transport: transport
                    .clone_with_binding(&binding)
                    .map_err(BybitAccountGatewayError::Transport)?,
                rules: rules.clone(),
            },
        )]);
        Ok(Self {
            runtime,
            binding,
            credentials,
            transport,
            identity,
            positions,
            rules,
            symbol_catalog,
            next_attempt_id: 2,
        })
    }

    fn take_attempt_id(&mut self) -> Result<u64, BybitAccountGatewayError> {
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(BybitAccountGatewayError::Attempt)?;
        Ok(attempt_id)
    }

    fn ensure_symbol_catalog(
        &mut self,
        symbols: &venue_execution::AccountSymbolSet,
    ) -> Result<(), BybitAccountGatewayError> {
        for symbol in symbols.iter() {
            if self.symbol_catalog.contains_key(symbol) {
                continue;
            }
            let binding = GatewayBinding::new(
                self.binding.gateway_binding().venue,
                self.binding.gateway_binding().mode,
                self.binding.gateway_binding().trading_account_id.clone(),
                symbol.clone(),
            )
            .map_err(|_| BybitAccountGatewayError::Binding)?;
            let binding =
                BybitGatewayBinding::new(binding).map_err(|_| BybitAccountGatewayError::Binding)?;
            let transport = self
                .transport
                .clone_with_binding(&binding)
                .map_err(BybitAccountGatewayError::Transport)?;
            let raw = self
                .runtime
                .block_on(transport.fetch_linear_instrument(&binding))
                .map_err(BybitAccountGatewayError::PublicTransport)?;
            let rules = parse_linear_instrument(&binding, raw)
                .map_err(|_| BybitAccountGatewayError::Instrument)?;
            self.symbol_catalog.insert(
                symbol.clone(),
                BybitSymbolScope {
                    binding,
                    transport,
                    rules,
                },
            );
        }
        Ok(())
    }

    fn refresh_private_for(&mut self, symbol: &Symbol) -> Result<(), BybitAccountGatewayError> {
        let (binding, generation) = self
            .symbol_catalog
            .get(symbol)
            .map(|scope| (scope.binding.clone(), scope.rules.instrument.generation))
            .ok_or(BybitAccountGatewayError::Binding)?;
        let attempt_id = self.take_attempt_id()?;
        let transport = &self
            .symbol_catalog
            .get(symbol)
            .ok_or(BybitAccountGatewayError::Binding)?
            .transport;
        let (identity, positions) = self.runtime.block_on(fetch_private_state(
            &binding,
            &self.credentials,
            transport,
            generation,
            attempt_id,
        ))?;
        self.identity = identity;
        self.positions = positions;
        Ok(())
    }

    fn refresh_rules_for(&mut self, symbol: &Symbol) -> Result<(), BybitAccountGatewayError> {
        let scope = self
            .symbol_catalog
            .get(symbol)
            .ok_or(BybitAccountGatewayError::Binding)?;
        let raw = self
            .runtime
            .block_on(scope.transport.fetch_linear_instrument(&scope.binding))
            .map_err(BybitAccountGatewayError::PublicTransport)?;
        let rules = parse_linear_instrument(&scope.binding, raw)
            .map_err(|_| BybitAccountGatewayError::Instrument)?;
        if rules.instrument.symbol != *symbol {
            return Err(BybitAccountGatewayError::Instrument);
        }
        self.symbol_catalog
            .get_mut(symbol)
            .ok_or(BybitAccountGatewayError::Binding)?
            .rules = rules.clone();
        if symbol == &self.binding.gateway_binding().symbol {
            self.rules = rules;
        }
        Ok(())
    }

    fn current_market_bbo_for(
        &mut self,
        symbol: &Symbol,
    ) -> Result<crate::BybitRestBbo, BybitAccountGatewayError> {
        let scope = self
            .symbol_catalog
            .get(symbol)
            .ok_or(BybitAccountGatewayError::Binding)?;
        let raw = self
            .runtime
            .block_on(scope.transport.fetch_linear_bbo(&scope.binding))
            .map_err(BybitAccountGatewayError::PublicTransport)?;
        parse_rest_bbo(&scope.binding, raw).map_err(|_| BybitAccountGatewayError::Instrument)
    }

    fn dispatch_permit(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        if permit.binding() != self.binding.gateway_binding() {
            return AccountGatewayResult::Rejected {
                reason: "bybit_permit_binding".to_owned(),
            };
        }
        let symbol = permit.command().mutation_owner().symbol.clone();
        if !self.symbol_catalog.contains_key(&symbol)
            || self.refresh_rules_for(&symbol).is_err()
            || self.refresh_private_for(&symbol).is_err()
        {
            return AccountGatewayResult::Rejected {
                reason: "bybit_symbol_preflight_failed".to_owned(),
            };
        }
        let (binding, rules) = match self.symbol_catalog.get(&symbol) {
            Some(scope) => (scope.binding.clone(), scope.rules.clone()),
            None => {
                return AccountGatewayResult::Rejected {
                    reason: "bybit_symbol_unconfigured".to_owned(),
                };
            }
        };
        let mut now_ms = match unix_ms() {
            Ok(value) => value,
            Err(_) => {
                return AccountGatewayResult::Rejected {
                    reason: "bybit_clock".to_owned(),
                };
            }
        };
        let request = match permit.command() {
            ExecutionCommand::PlaceLimit(command) => {
                if !command.reduce_only
                    && self
                        .positions
                        .positions
                        .iter()
                        .any(|position| !position.position.quantity.is_zero())
                {
                    return AccountGatewayResult::Rejected {
                        reason: "bybit_existing_position".to_owned(),
                    };
                }
                prepare_place_request(
                    &binding,
                    &self.identity,
                    &rules,
                    &BybitPlaceIntent {
                        client_order_id: command.client_order_id.as_str().to_owned(),
                        side: command.side,
                        position_side: command.position_side,
                        kind: BybitOrderKind::Limit,
                        quantity: command.quantity,
                        limit_price: Some(command.limit_price),
                        time_in_force: BybitTimeInForce::PostOnly,
                        reduce_only: command.reduce_only,
                    },
                    now_ms,
                    None,
                )
            }
            ExecutionCommand::Cancel(command) => prepare_cancel_request(
                &binding,
                &self.identity,
                &rules,
                &BybitCancelIntent {
                    order_id: None,
                    client_order_id: Some(command.target_client_order_id.as_str().to_owned()),
                },
            ),
            ExecutionCommand::MarketReduce(command) => {
                if validate_market_reduce_position(command, &self.positions, &symbol).is_err() {
                    return AccountGatewayResult::Rejected {
                        reason: "bybit_market_reduce_position".to_owned(),
                    };
                }
                let bbo = match self.current_market_bbo_for(&symbol) {
                    Ok(value) => value,
                    Err(_) => {
                        return AccountGatewayResult::Rejected {
                            reason: "bybit_market_reduce_rules".to_owned(),
                        };
                    }
                };
                now_ms = match unix_ms() {
                    Ok(value) => value,
                    Err(_) => {
                        return AccountGatewayResult::Rejected {
                            reason: "bybit_clock".to_owned(),
                        };
                    }
                };
                prepare_place_request(
                    &binding,
                    &self.identity,
                    &rules,
                    &BybitPlaceIntent {
                        client_order_id: command.client_order_id.as_str().to_owned(),
                        side: command.side,
                        position_side: command.position_side,
                        kind: BybitOrderKind::Market,
                        quantity: command.quantity,
                        limit_price: None,
                        time_in_force: BybitTimeInForce::ImmediateOrCancel,
                        reduce_only: true,
                    },
                    now_ms,
                    Some(&bbo),
                )
            }
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                return AccountGatewayResult::Rejected {
                    reason: "bybit_initial_profile_unsupported_command".to_owned(),
                };
            }
        };
        let request = match request {
            Ok(value) => value,
            Err(_) => {
                return AccountGatewayResult::Rejected {
                    reason: "bybit_intent_rejected".to_owned(),
                };
            }
        };
        let outcome = {
            let transport = &self
                .symbol_catalog
                .get(&symbol)
                .ok_or(())
                .map(|scope| &scope.transport);
            let Ok(transport) = transport else {
                return AccountGatewayResult::Rejected {
                    reason: "bybit_symbol_unconfigured".to_owned(),
                };
            };
            self.runtime.block_on(transport.execute_order(
                &binding,
                &self.credentials,
                &request,
                now_ms,
            ))
        };
        match outcome {
            Ok(ack) => {
                if !matches!(permit.command(), ExecutionCommand::MarketReduce(_)) {
                    return ack
                        .order_id
                        .or(ack.client_order_id)
                        .map_or(AccountGatewayResult::Unknown, |venue_order_id| {
                            AccountGatewayResult::Accepted { venue_order_id }
                        });
                }
                let lookup = match ack.client_order_id.as_ref() {
                    Some(value) => BybitOrderLookup::by_client_order_id(value.clone()),
                    None => Err(crate::BybitError::Payload),
                };
                let Ok(lookup) = lookup else {
                    return AccountGatewayResult::Unknown;
                };
                let attempt_id = match self.take_attempt_id() {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let readback_now = match unix_ms() {
                    Ok(value) => value,
                    Err(_) => return AccountGatewayResult::Unknown,
                };
                let transport = match self.symbol_catalog.get(&symbol) {
                    Some(scope) => &scope.transport,
                    None => return AccountGatewayResult::Unknown,
                };
                let readback = self.runtime.block_on(fetch_exact_readback(
                    &binding,
                    &self.credentials,
                    transport,
                    rules.instrument.generation,
                    attempt_id,
                    lookup,
                    readback_now,
                ));
                let Ok(readback) = readback else {
                    return AccountGatewayResult::Unknown;
                };
                match settle_order_ack(&binding, &ack, &readback) {
                    Ok(settlement) if settlement.state == OrderState::Rejected => {
                        AccountGatewayResult::Rejected {
                            reason: "bybit_market_reduce_rejected".to_owned(),
                        }
                    }
                    Ok(settlement) => AccountGatewayResult::Accepted {
                        venue_order_id: settlement.order_id,
                    },
                    Err(_) => AccountGatewayResult::Unknown,
                }
            }
            Err(BybitTransportError::Rejected) => AccountGatewayResult::Rejected {
                reason: "bybit_venue_rejected".to_owned(),
            },
            Err(
                BybitTransportError::Binding
                | BybitTransportError::Signing
                | BybitTransportError::BodyTooLarge
                | BybitTransportError::Limits,
            ) => AccountGatewayResult::Rejected {
                reason: "bybit_pre_send_rejected".to_owned(),
            },
            Err(_) => AccountGatewayResult::Unknown,
        }
    }
}

fn validate_market_reduce_position(
    command: &MarketReduceCommand,
    positions: &BybitPositionReadback,
    symbol: &Symbol,
) -> Result<(), ()> {
    let position = positions
        .positions
        .iter()
        .find(|value| {
            value.position.symbol == *symbol && value.position.side == command.position_side
        })
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

fn normalize_limit_from_bbo(
    binding: &BybitGatewayBinding,
    rules: &BybitLinearInstrumentRules,
    intent: &AccountLimitNormalizationIntent,
    bbo: &crate::BybitRestBbo,
    now_ms: u64,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    intent.validate()?;
    if intent.owner.exchange != binding.gateway_binding().venue.as_str()
        || intent.owner.account != binding.gateway_binding().trading_account_id
        || intent.owner.symbol != binding.gateway_binding().symbol
        || !matches!(
            (intent.position_side, intent.side, intent.reduce_only),
            (PositionSide::Long, OrderSide::Buy, false)
                | (PositionSide::Long, OrderSide::Sell, true)
                | (PositionSide::Short, OrderSide::Sell, false)
                | (PositionSide::Short, OrderSide::Buy, true)
        )
        || rules.raw.binding != *binding.gateway_binding()
        || rules.instrument.symbol != binding.gateway_binding().symbol
        || bbo.raw.binding != *binding.gateway_binding()
        || bbo.raw.generation != rules.instrument.generation
        || bbo.snapshot.generation != rules.instrument.generation
        || now_ms < bbo.raw.received_at_ms
        || now_ms.saturating_sub(bbo.raw.received_at_ms) > LIMIT_BBO_MAX_AGE_MS
    {
        return Err(AccountHostValidationError::Command);
    }
    let raw_price = match intent.side {
        OrderSide::Buy => bbo.snapshot.bids.first(),
        OrderSide::Sell => bbo.snapshot.asks.first(),
    }
    .map(|level| level.price.value())
    .ok_or(AccountHostValidationError::Command)?;
    let price = Price::new(floor_to_step(
        raw_price,
        rules.instrument.price_tick.value(),
    )?)
    .map_err(|_| AccountHostValidationError::Command)?;
    if price < rules.minimum_price || price > rules.maximum_price {
        return Err(AccountHostValidationError::Command);
    }
    let raw_quantity = intent
        .quote_delta
        .checked_div(price.value())
        .ok_or(AccountHostValidationError::Command)?;
    let quantity = floor_to_step(raw_quantity, rules.instrument.quantity_step)?;
    let notional = quantity
        .checked_mul(price.value())
        .ok_or(AccountHostValidationError::Notional)?;
    if quantity < rules.minimum_quantity
        || quantity > rules.maximum_limit_quantity
        || notional < rules.instrument.minimum_notional.value
        || notional > intent.quote_delta
    {
        return Err(AccountHostValidationError::Command);
    }
    let command = ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: intent.command_id.clone(),
        client_order_id: intent.client_order_id.clone(),
        owner: intent.owner.clone(),
        side: intent.side,
        position_side: intent.position_side,
        quantity,
        limit_price: price,
        reduce_only: intent.reduce_only,
    });
    command
        .validate()
        .map_err(|_| AccountHostValidationError::Command)?;
    Ok(command)
}

fn floor_to_step(
    value: rust_decimal::Decimal,
    step: rust_decimal::Decimal,
) -> Result<rust_decimal::Decimal, AccountHostValidationError> {
    if value <= rust_decimal::Decimal::ZERO || step <= rust_decimal::Decimal::ZERO {
        return Err(AccountHostValidationError::Command);
    }
    let floored = value - value % step;
    if floored <= rust_decimal::Decimal::ZERO {
        return Err(AccountHostValidationError::Command);
    }
    Ok(floored)
}

impl AccountPhysicalGateway for BybitAccountGateway {
    type Error = BybitAccountGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.binding.gateway_binding()
    }

    fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        let raw = self
            .runtime
            .block_on(self.transport.fetch_linear_instrument(&self.binding))
            .map_err(|_| AccountHostValidationError::Instrument)?;
        let current = parse_linear_instrument(&self.binding, raw)
            .map_err(|_| AccountHostValidationError::Instrument)?;
        // The transport binds every public response to the resident's rules generation. A
        // semantic instrument change is fail-closed instead of being relabelled with that old
        // generation.
        if current.instrument != self.rules.instrument {
            return Err(AccountHostValidationError::Instrument);
        }
        current
            .instrument
            .validate()
            .map_err(|_| AccountHostValidationError::Instrument)?;
        Ok(AccountInstrumentIdentity {
            identity: current.instrument.identity(),
            rules_generation: current.instrument.generation,
        })
    }

    fn current_instrument_for(
        &mut self,
        symbol: &Symbol,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        let scope = self
            .symbol_catalog
            .get(symbol)
            .ok_or(AccountHostValidationError::Instrument)?;
        let raw = self
            .runtime
            .block_on(scope.transport.fetch_linear_instrument(&scope.binding))
            .map_err(|_| AccountHostValidationError::Instrument)?;
        let current = parse_linear_instrument(&scope.binding, raw)
            .map_err(|_| AccountHostValidationError::Instrument)?;
        if current.instrument != scope.rules.instrument || current.instrument.symbol != *symbol {
            return Err(AccountHostValidationError::Instrument);
        }
        Ok(AccountInstrumentIdentity {
            identity: current.instrument.identity(),
            rules_generation: current.instrument.generation,
        })
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        if request.binding() != self.binding.gateway_binding() {
            return Err(BybitAccountGatewayError::Binding);
        }
        self.ensure_symbol_catalog(request.configured_symbols())?;
        self.refresh_private_for(&self.binding.gateway_binding().symbol.clone())?;
        let observed_at_ms = unix_ms().map_err(BybitAccountGatewayError::Transport)?;
        let mut outcomes = Vec::with_capacity(request.unresolved().len());
        for command in request.unresolved() {
            let lookup_id = match command {
                ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
                _ => command
                    .native_client_id()
                    .ok_or(BybitAccountGatewayError::Readback)?
                    .as_str(),
            };
            let lookup = BybitOrderLookup::by_client_order_id(lookup_id.to_owned())
                .map_err(|_| BybitAccountGatewayError::Readback)?;
            let attempt_id = self.take_attempt_id()?;
            let scope = self
                .symbol_catalog
                .get(&command.mutation_owner().symbol)
                .ok_or(BybitAccountGatewayError::Binding)?;
            let readback = self.runtime.block_on(fetch_exact_readback(
                &scope.binding,
                &self.credentials,
                &scope.transport,
                scope.rules.instrument.generation,
                attempt_id,
                lookup,
                observed_at_ms,
            ))?;
            let outcome = recovery_outcome(command, &readback)?;
            outcomes.push(outcome);
        }
        AccountRecoveryReport::new(
            self.binding.gateway_binding().clone(),
            observed_at_ms,
            outcomes,
        )
        .map_err(|_| BybitAccountGatewayError::Readback)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        let attempt_id = self
            .take_attempt_id()
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        self.runtime.block_on(collect_account_wide_risk(
            &self.binding,
            &self.credentials,
            &self.transport,
            self.rules.instrument.generation,
            attempt_id,
        ))
    }

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        self.ensure_symbol_catalog(request.configured_symbols())
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let attempt_id = self
            .take_attempt_id()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        self.runtime.block_on(collect_account_wide_snapshot(
            &self.binding,
            &self.credentials,
            &self.transport,
            self.rules.instrument.generation,
            attempt_id,
            request,
        ))
    }

    fn normalize_limit_intent(
        &mut self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        let scope = self
            .symbol_catalog
            .get(&intent.owner.symbol)
            .ok_or(AccountHostValidationError::Scope)?;
        let raw = self
            .runtime
            .block_on(scope.transport.fetch_linear_bbo(&scope.binding))
            .map_err(|_| AccountHostValidationError::Command)?;
        let bbo =
            parse_rest_bbo(&scope.binding, raw).map_err(|_| AccountHostValidationError::Command)?;
        let now_ms = unix_ms().map_err(|_| AccountHostValidationError::Command)?;
        normalize_limit_from_bbo(&scope.binding, &scope.rules, intent, &bbo, now_ms)
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatch_permit(permit)
    }
}

async fn collect_account_wide_snapshot(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
    recovery: &AccountRecoveryRequest,
) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
    let account = account_wide_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountInfo,
        None,
    )
    .await?;
    let wallet = account_wide_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::WalletBalance,
        None,
    )
    .await?;
    let positions = account_wide_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountWidePositions,
        None,
    )
    .await?;
    let regular = account_wide_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountWideOpenOrders(NativeOrderFamily::UmOrder),
        None,
    )
    .await?;
    let conditional = account_wide_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountWideOpenOrders(NativeOrderFamily::UmConditional),
        None,
    )
    .await?;
    let now = unix_ms().map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let history = fills_history_window(now, recovery.previous_fills_cursor())?;
    let fills = account_wide_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountWideExecutions,
        Some(history),
    )
    .await?;
    let position_facts = snapshot_positions(&positions)?;
    let order_facts = snapshot_orders(&regular, &conditional)?;
    let (fills, fills_cursor) = snapshot_fills(&fills, recovery.previous_fills_cursor())?;
    let mut unknown_results = Vec::with_capacity(recovery.unresolved().len());
    for command in recovery.unresolved() {
        let identity = match command {
            ExecutionCommand::Cancel(value) => value.target_client_order_id.as_str(),
            _ => command
                .native_client_id()
                .ok_or(AccountHostValidationError::SignedSnapshot)?
                .as_str(),
        };
        let readback = fetch_exact_readback(
            binding,
            credentials,
            transport,
            generation,
            attempt_id,
            BybitOrderLookup::by_client_order_id(identity.to_owned())
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            now,
        )
        .await
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let result = match recovery_outcome(command, &readback)
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?
        {
            value
                if matches!(
                    value.state(),
                    venue_execution::AccountRecoveryState::Accepted { .. }
                ) =>
            {
                let venue_order_id = match value.state() {
                    venue_execution::AccountRecoveryState::Accepted { venue_order_id } => {
                        venue_order_id.clone()
                    }
                    _ => return Err(AccountHostValidationError::SignedSnapshot),
                };
                SignedUnknownResult::Accepted { venue_order_id }
            }
            value
                if matches!(
                    value.state(),
                    venue_execution::AccountRecoveryState::Rejected { .. }
                ) =>
            {
                let reason = match value.state() {
                    venue_execution::AccountRecoveryState::Rejected { reason } => reason.clone(),
                    _ => return Err(AccountHostValidationError::SignedSnapshot),
                };
                SignedUnknownResult::Rejected { reason }
            }
            _ => SignedUnknownResult::Unknown,
        };
        unknown_results.push(SignedUnknownFact {
            command_id: command.command_id().clone(),
            result,
        });
    }
    let identity = crate::parse_account_identity(
        binding,
        account
            .first()
            .ok_or(AccountHostValidationError::SignedSnapshot)?,
    )
    .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let balances = crate::parse_unified_wallet(
        binding,
        &identity,
        wallet
            .first()
            .ok_or(AccountHostValidationError::SignedSnapshot)?,
    )
    .map_err(|_| AccountHostValidationError::SignedSnapshot)?
    .coins
    .into_iter()
    .map(|coin| SignedAccountBalance {
        asset: coin.asset,
        equity: coin.equity,
        available_margin: None,
    })
    .collect();
    SignedAccountSnapshot::complete_with_fills(
        binding.gateway_binding().clone(),
        now,
        generation,
        attempt_id,
        generation,
        SignedAccountPositionMode::Hedge,
        order_facts,
        position_facts,
        fills,
        fills_cursor,
        unknown_results,
    )
    .map_err(|_| AccountHostValidationError::SignedSnapshot)?
    .with_balances(balances)
}

fn snapshot_fills(
    raws: &[crate::BybitRawPrivatePayload],
    previous_cursor: Option<&str>,
) -> Result<(Vec<Fill>, String), AccountHostValidationError> {
    if raws.is_empty() || raws.len() > crate::BYBIT_PRIVATE_MAX_PAGES {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let mut cursor_chain = std::collections::BTreeSet::new();
    let mut fill_ids = std::collections::BTreeSet::new();
    let mut fills = Vec::new();
    let mut previous_time = None;
    let mut cursor = None;
    let mut expected_cursor = None;
    for (index, raw) in raws.iter().enumerate() {
        if raw.source != BybitPrivateSource::AccountWideExecutions
            || usize::try_from(raw.page_index).ok() != Some(index)
            || raw.request_cursor != expected_cursor
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let rows = response_list(&raw.payload)?;
        let next = response_cursor(&raw.payload)?;
        if rows.len() == 100 && next.is_none()
            || next
                .as_deref()
                .is_some_and(|value| !cursor_chain.insert(value.to_owned()))
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        if index + 1 == raws.len() && next.is_some() {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        expected_cursor = next;
        for row in rows {
            let fill_id = text(&row, "execId")?.to_owned();
            let order_id = text(&row, "orderId")?.to_owned();
            let native = text(&row, "symbol")?;
            let base = native
                .strip_suffix("USDT")
                .filter(|value| !value.is_empty())
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            let side = match text(&row, "side")? {
                "Buy" => OrderSide::Buy,
                "Sell" => OrderSide::Sell,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            let time = text(&row, "execTime")?
                .parse::<u64>()
                .ok()
                .filter(|value| *value > 0 && *value <= raw.received_at_ms)
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            let fee_asset = Asset::new(text(&row, "feeCurrency")?)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let fill = Fill {
                fill_id: fill_id.clone(),
                execution_sequence: FieldState::Known(
                    text(&row, "seq")?
                        .parse::<u64>()
                        .ok()
                        .filter(|value| *value > 0)
                        .ok_or(AccountHostValidationError::SignedSnapshot)?,
                ),
                order_id,
                symbol: Symbol::new(base, "USDT")
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                side,
                position_side: FieldState::Missing,
                quantity: decimal(row.get("execQty"))?,
                price: Price::new(decimal(row.get("execPrice"))?)
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                fee: FieldState::Known(Amount::new(fee_asset, decimal(row.get("execFee"))?)),
                realized_pnl: row
                    .get("execPnl")
                    .and_then(serde_json::Value::as_str)
                    .map(|value| {
                        Ok(FieldState::Known(Amount::new(
                            Asset::new(text(&row, "feeCurrency")?)
                                .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                            rust_decimal::Decimal::from_str(value)
                                .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                        )))
                    })
                    .transpose()?
                    .unwrap_or(FieldState::Missing),
                maker: FieldState::Known(
                    row.get("isMaker")
                        .and_then(serde_json::Value::as_bool)
                        .ok_or(AccountHostValidationError::SignedSnapshot)?,
                ),
                exchange_time_ms: Some(time),
            };
            fill.validate()
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            if fill_ids.insert(fill_id) {
                if previous_time.is_some_and(|previous| time > previous) {
                    return Err(AccountHostValidationError::SignedSnapshot);
                }
                previous_time = Some(time);
                let candidate = (time, fill.fill_id.clone());
                if cursor.as_ref().is_none_or(|value: &String| {
                    cursor_parts(value).is_ok_and(|current| candidate > current)
                }) {
                    cursor = Some(format!("bybit-exec:{time}:{}", fill.fill_id));
                }
                fills.push(fill);
            }
        }
    }
    Ok((
        fills,
        cursor
            .or_else(|| previous_cursor.map(str::to_owned))
            .unwrap_or_else(|| {
                format!(
                    "bybit-exec:{}:empty",
                    raws[0]
                        .history_window
                        .as_ref()
                        .map_or(0, |window| window.end_ms)
                )
            }),
    ))
}

fn fills_history_window(
    now_ms: u64,
    previous_cursor: Option<&str>,
) -> Result<BybitHistoryWindow, AccountHostValidationError> {
    let start_ms = match previous_cursor {
        None => now_ms.saturating_sub(HISTORY_WINDOW_MS).max(1),
        Some(cursor) => {
            let (watermark, _) = cursor_parts(cursor)?;
            if watermark > now_ms || now_ms.saturating_sub(watermark) > HISTORY_WINDOW_MS {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            watermark.saturating_sub(FILLS_CURSOR_OVERLAP_MS).max(1)
        }
    };
    BybitHistoryWindow::new(start_ms, now_ms)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

fn cursor_parts(cursor: &str) -> Result<(u64, String), AccountHostValidationError> {
    if let Some(time) = cursor.strip_prefix("bybit-exec:empty:") {
        let time = time
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        return Ok((time, "empty".to_owned()));
    }
    let Some(value) = cursor.strip_prefix("bybit-exec:") else {
        return Err(AccountHostValidationError::SignedSnapshot);
    };
    let Some((time, fill_id)) = value.split_once(':') else {
        return Err(AccountHostValidationError::SignedSnapshot);
    };
    let time = time
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    if fill_id.is_empty() || fill_id.len() > 128 || fill_id.chars().any(char::is_control) {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    Ok((time, fill_id.to_owned()))
}

async fn collect_account_wide_risk(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
) -> Result<AccountRiskEvidence, AccountHostValidationError> {
    let started_at_ms = unix_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?;
    // The V5 profile has complete normal and conditional namespaces but no independent Algo
    // list. Its absence is explicitly profile evidence, so fresh aggregate evidence can only be
    // admitted once the two visible families and every position page have closed above.
    let positions = account_wide_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountWidePositions,
        None,
    )
    .await?;
    let regular = account_wide_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountWideOpenOrders(NativeOrderFamily::UmOrder),
        None,
    )
    .await?;
    let conditional = account_wide_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountWideOpenOrders(NativeOrderFamily::UmConditional),
        None,
    )
    .await?;
    AccountRiskEvidence::complete(
        binding.gateway_binding().clone(),
        unix_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?,
        generation,
        position_notionals(&positions)?,
        entry_order_notionals(&regular, &conditional)?,
    )?
    .with_earliest_observation(started_at_ms)
}

async fn account_wide_pages(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
    source: BybitPrivateSource,
    history: Option<BybitHistoryWindow>,
) -> Result<Vec<crate::BybitRawPrivatePayload>, AccountHostValidationError> {
    let mut pages = Vec::new();
    let mut cursor = None;
    for page_index in 0..crate::BYBIT_PRIVATE_MAX_PAGES {
        let raw = fetch_one(
            binding,
            credentials,
            transport,
            generation,
            attempt_id,
            source,
            u32::try_from(page_index).map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            cursor.as_deref(),
            history.clone(),
        )
        .await
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        cursor = response_cursor(&raw.payload)?;
        pages.push(raw);
        if cursor.is_none() {
            return Ok(pages);
        }
    }
    Err(AccountHostValidationError::SignedSnapshot)
}

fn response_cursor(payload: &[u8]) -> Result<Option<String>, AccountHostValidationError> {
    let root: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let result = root
        .get("result")
        .and_then(serde_json::Value::as_object)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    match result.get("nextPageCursor") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) if value.is_empty() => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value.clone())),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn snapshot_positions(
    raws: &[crate::BybitRawPrivatePayload],
) -> Result<Vec<SignedAccountPositionFact>, AccountHostValidationError> {
    let mut facts = Vec::new();
    for raw in raws {
        for row in response_list(&raw.payload)? {
            let native = text(&row, "symbol")?;
            let base = native
                .strip_suffix("USDT")
                .filter(|base| !base.is_empty())
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            let quantity = decimal(row.get("size"))?;
            let index = row
                .get("positionIdx")
                .and_then(serde_json::Value::as_u64)
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            let side = match index {
                1 => PositionSide::Long,
                2 => PositionSide::Short,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            facts.push(SignedAccountPositionFact {
                symbol: Symbol::new(base, "USDT")
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                position_side: side,
                quantity,
                entry_price: optional_decimal(row.get("avgPrice"))?,
                mark_price: optional_decimal(row.get("markPrice"))?,
            });
        }
    }
    Ok(facts)
}

fn snapshot_orders(
    regular: &[crate::BybitRawPrivatePayload],
    conditional: &[crate::BybitRawPrivatePayload],
) -> Result<Vec<SignedAccountOrderFact>, AccountHostValidationError> {
    let orders = response_rows(regular)?
        .into_iter()
        .map(|row| (row, NativeOrderFamily::UmOrder))
        .chain(
            response_rows(conditional)?
                .into_iter()
                .map(|row| (row, NativeOrderFamily::UmConditional)),
        );
    orders
        .map(|(row, family)| {
            let native = text(&row, "symbol")?;
            let base = native
                .strip_suffix("USDT")
                .filter(|base| !base.is_empty())
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            let side = match text(&row, "side")? {
                "Buy" => venue_domain::domain::OrderSide::Buy,
                "Sell" => venue_domain::domain::OrderSide::Sell,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            let position_side = match row.get("positionIdx").and_then(serde_json::Value::as_u64) {
                Some(1) => PositionSide::Long,
                Some(2) => PositionSide::Short,
                _ => return Err(AccountHostValidationError::SignedSnapshot),
            };
            Ok(SignedAccountOrderFact {
                client_order_id: text(&row, "orderLinkId")?.to_owned(),
                venue_order_id: Some(text(&row, "orderId")?.to_owned()),
                symbol: Symbol::new(base, "USDT")
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                family,
                side,
                position_side,
                quantity: decimal(row.get("leavesQty"))?,
                limit_price: optional_decimal(row.get("price"))?,
                reduce_only: row
                    .get("reduceOnly")
                    .and_then(serde_json::Value::as_bool)
                    .ok_or(AccountHostValidationError::SignedSnapshot)?,
                owner: None,
                external: true,
                state: Some(bybit_order_state(text(&row, "orderStatus")?)?),
                filled_quantity: Some(decimal(row.get("cumExecQty"))?),
            })
        })
        .collect()
}

fn bybit_order_state(
    value: &str,
) -> Result<venue_domain::domain::OrderState, AccountHostValidationError> {
    match value {
        "New" | "Untriggered" => Ok(venue_domain::domain::OrderState::New),
        "PartiallyFilled" => Ok(venue_domain::domain::OrderState::PartiallyFilled),
        "Filled" => Ok(venue_domain::domain::OrderState::Filled),
        "Cancelled" | "Deactivated" => Ok(venue_domain::domain::OrderState::Cancelled),
        "Rejected" => Ok(venue_domain::domain::OrderState::Rejected),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn position_notionals(
    raws: &[crate::BybitRawPrivatePayload],
) -> Result<Vec<rust_decimal::Decimal>, AccountHostValidationError> {
    response_rows(raws)?
        .into_iter()
        .map(|row| {
            let quantity = decimal(row.get("size"))?;
            let mark = decimal(row.get("markPrice"))?;
            if quantity < rust_decimal::Decimal::ZERO || mark <= rust_decimal::Decimal::ZERO {
                return Err(AccountHostValidationError::RiskEvidence);
            }
            quantity
                .checked_mul(mark)
                .ok_or(AccountHostValidationError::Notional)
        })
        .filter(|v| !matches!(v, Ok(v) if v.is_zero()))
        .collect()
}

fn entry_order_notionals(
    regular: &[crate::BybitRawPrivatePayload],
    conditional: &[crate::BybitRawPrivatePayload],
) -> Result<Vec<rust_decimal::Decimal>, AccountHostValidationError> {
    response_rows(regular)?
        .into_iter()
        .chain(response_rows(conditional)?)
        .map(|row| {
            let reduce = row
                .get("reduceOnly")
                .and_then(serde_json::Value::as_bool)
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            if reduce {
                return Ok(rust_decimal::Decimal::ZERO);
            }
            let left = decimal(row.get("leavesQty"))?;
            let price = decimal(row.get("price"))?;
            if left <= rust_decimal::Decimal::ZERO || price <= rust_decimal::Decimal::ZERO {
                return Err(AccountHostValidationError::RiskEvidence);
            }
            left.checked_mul(price)
                .ok_or(AccountHostValidationError::Notional)
        })
        .filter(|v| !matches!(v, Ok(v) if v.is_zero()))
        .collect()
}

fn response_rows(
    raws: &[crate::BybitRawPrivatePayload],
) -> Result<Vec<serde_json::Value>, AccountHostValidationError> {
    raws.iter()
        .map(|raw| response_list(&raw.payload))
        .collect::<Result<Vec<_>, _>>()
        .map(|pages| pages.into_iter().flatten().collect())
}
fn response_list(payload: &[u8]) -> Result<Vec<serde_json::Value>, AccountHostValidationError> {
    let root: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    if root.get("retCode").and_then(serde_json::Value::as_i64) != Some(0) {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    root.get("result")
        .and_then(serde_json::Value::as_object)
        .and_then(|r| r.get("list"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or(AccountHostValidationError::SignedSnapshot)
}
fn text<'a>(
    row: &'a serde_json::Value,
    field: &str,
) -> Result<&'a str, AccountHostValidationError> {
    row.get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or(AccountHostValidationError::SignedSnapshot)
}
fn decimal(
    value: Option<&serde_json::Value>,
) -> Result<rust_decimal::Decimal, AccountHostValidationError> {
    match value {
        Some(serde_json::Value::String(v)) => v
            .parse()
            .map_err(|_| AccountHostValidationError::SignedSnapshot),
        Some(serde_json::Value::Number(v)) => v
            .to_string()
            .parse()
            .map_err(|_| AccountHostValidationError::SignedSnapshot),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}
fn optional_decimal(
    value: Option<&serde_json::Value>,
) -> Result<Option<rust_decimal::Decimal>, AccountHostValidationError> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(value)) if value.is_empty() || value == "0" => Ok(None),
        value => decimal(value).map(Some),
    }
}

async fn bootstrap(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
) -> Result<
    (
        BybitLinearInstrumentRules,
        BybitAccountIdentity,
        BybitPositionReadback,
    ),
    BybitAccountGatewayError,
> {
    let raw = transport
        .fetch_linear_instrument(binding)
        .await
        .map_err(BybitAccountGatewayError::PublicTransport)?;
    let rules =
        parse_linear_instrument(binding, raw).map_err(|_| BybitAccountGatewayError::Instrument)?;
    let api_key = fetch_one(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::ApiKeyInfo,
        0,
        None,
        None,
    )
    .await?;
    let api_key = parse_api_key_evidence(binding, credentials, &api_key)
        .map_err(|_| BybitAccountGatewayError::Permissions)?;
    if api_key.read_only
        || !api_key.contract_order
        || !api_key.contract_position
        || api_key.withdraw
    {
        return Err(BybitAccountGatewayError::Permissions);
    }
    let (identity, positions) =
        fetch_private_state(binding, credentials, transport, generation, attempt_id).await?;
    Ok((rules, identity, positions))
}

async fn fetch_private_state(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
) -> Result<(BybitAccountIdentity, BybitPositionReadback), BybitAccountGatewayError> {
    let account = fetch_one(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::AccountInfo,
        0,
        None,
        None,
    )
    .await?;
    let identity = parse_account_identity(binding, &account)
        .map_err(|_| BybitAccountGatewayError::AccountIdentity)?;
    if !identity.mode.supports_unified_wallet() {
        return Err(BybitAccountGatewayError::AccountMode);
    }
    let raw = fetch_one(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::Positions,
        0,
        None,
        None,
    )
    .await?;
    if !has_both_hedge_legs(&raw)? {
        return Err(BybitAccountGatewayError::PositionMode);
    }
    let page = parse_position_page(binding, &raw).map_err(|_| {
        BybitAccountGatewayError::PositionPayload(diagnose_position_page(binding, &raw))
    })?;
    let positions = BybitPositionReadback {
        raw_pages: vec![raw.clone()],
        binding: raw.binding.clone(),
        generation: raw.generation,
        attempt_id: raw.attempt_id,
        observed_at_ms: raw.received_at_ms,
        hedge_mode: true,
        positions: page.positions,
    };
    Ok((identity, positions))
}

fn has_both_hedge_legs(
    raw: &crate::BybitRawPrivatePayload,
) -> Result<bool, BybitAccountGatewayError> {
    #[derive(Deserialize)]
    struct Envelope {
        #[serde(rename = "retCode")]
        ret_code: i64,
        result: ResultRows,
    }
    #[derive(Deserialize)]
    struct ResultRows {
        list: Vec<PositionIndex>,
    }
    #[derive(Deserialize)]
    struct PositionIndex {
        #[serde(rename = "positionIdx")]
        position_idx: u8,
    }

    let envelope: Envelope =
        serde_json::from_slice(&raw.payload).map_err(|_| BybitAccountGatewayError::Positions)?;
    if envelope.ret_code != 0 {
        return Err(BybitAccountGatewayError::Positions);
    }
    let indexes = envelope
        .result
        .list
        .into_iter()
        .map(|row| row.position_idx)
        .collect::<std::collections::BTreeSet<_>>();
    Ok(indexes == std::collections::BTreeSet::from([1, 2]))
}

#[allow(clippy::too_many_arguments)]
async fn fetch_one(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
    source: BybitPrivateSource,
    page_index: u32,
    cursor: Option<&str>,
    history_window: Option<BybitHistoryWindow>,
) -> Result<crate::BybitRawPrivatePayload, BybitAccountGatewayError> {
    let request = prepare_private_request(
        binding,
        generation,
        attempt_id,
        page_index,
        source,
        cursor,
        history_window,
        None,
    )
    .map_err(|_| BybitAccountGatewayError::Readback)?;
    execute_private(binding, credentials, transport, &request)
        .await
        .map_err(|error| match source {
            BybitPrivateSource::ApiKeyInfo => BybitAccountGatewayError::ApiKeyTransport(error),
            BybitPrivateSource::AccountInfo | BybitPrivateSource::WalletBalance => {
                BybitAccountGatewayError::AccountTransport(error)
            }
            BybitPrivateSource::Positions => BybitAccountGatewayError::PositionTransport(error),
            BybitPrivateSource::AccountWidePositions => {
                BybitAccountGatewayError::PositionTransport(error)
            }
            BybitPrivateSource::OpenOrders(_)
            | BybitPrivateSource::AccountWideOpenOrders(_)
            | BybitPrivateSource::OrderHistory(_)
            | BybitPrivateSource::Executions
            | BybitPrivateSource::AccountWideExecutions => {
                BybitAccountGatewayError::OrderTransport(error)
            }
        })
}

async fn execute_private(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    request: &BybitPreparedPrivateRequest,
) -> Result<crate::BybitRawPrivatePayload, BybitTransportError> {
    let now_ms = unix_ms()?;
    transport
        .execute_private_read(binding, credentials, request, now_ms)
        .await
}

async fn fetch_exact_readback(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
    lookup: BybitOrderLookup,
    now_ms: u64,
) -> Result<BybitClosedOrderReadback, BybitAccountGatewayError> {
    let history_window =
        BybitHistoryWindow::new(now_ms.saturating_sub(HISTORY_WINDOW_MS).max(1), now_ms)
            .map_err(|_| BybitAccountGatewayError::Readback)?;
    let open = fetch_order_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::OpenOrders(NativeOrderFamily::UmOrder),
        None,
        &lookup,
    )
    .await?;
    let history = fetch_order_pages(
        binding,
        credentials,
        transport,
        generation,
        attempt_id,
        BybitPrivateSource::OrderHistory(NativeOrderFamily::UmOrder),
        Some(history_window),
        &lookup,
    )
    .await?;
    let open = open
        .iter()
        .map(|raw| parse_open_order_page(binding, raw))
        .collect::<Result<Vec<BybitOpenOrderPage>, _>>()
        .map_err(|_| BybitAccountGatewayError::Readback)?;
    let history = history
        .iter()
        .map(|raw| parse_order_history_page(binding, raw))
        .collect::<Result<Vec<BybitOrderEvidencePage>, _>>()
        .map_err(|_| BybitAccountGatewayError::Readback)?;
    BybitClosedOrderReadback::from_pages(binding, generation, &open, &history)
        .map_err(|_| BybitAccountGatewayError::Readback)
}

#[allow(clippy::too_many_arguments)]
async fn fetch_order_pages(
    binding: &BybitGatewayBinding,
    credentials: &BybitCredentials,
    transport: &BybitHttpTransport,
    generation: u64,
    attempt_id: u64,
    source: BybitPrivateSource,
    history_window: Option<BybitHistoryWindow>,
    lookup: &BybitOrderLookup,
) -> Result<Vec<crate::BybitRawPrivatePayload>, BybitAccountGatewayError> {
    let mut pages = Vec::new();
    let mut cursor = None;
    for page_index in 0..EXACT_READBACK_MAX_PAGES {
        let request = prepare_private_request(
            binding,
            generation,
            attempt_id,
            page_index,
            source,
            cursor.as_deref(),
            history_window.clone(),
            Some(lookup.clone()),
        )
        .map_err(|_| BybitAccountGatewayError::Readback)?;
        let raw = execute_private(binding, credentials, transport, &request)
            .await
            .map_err(BybitAccountGatewayError::OrderTransport)?;
        cursor = match source {
            BybitPrivateSource::OpenOrders(_) => {
                parse_open_order_page(binding, &raw)
                    .map_err(|_| BybitAccountGatewayError::Readback)?
                    .meta
                    .next_cursor
            }
            BybitPrivateSource::OrderHistory(_) => {
                parse_order_history_page(binding, &raw)
                    .map_err(|_| BybitAccountGatewayError::Readback)?
                    .meta
                    .next_cursor
            }
            _ => return Err(BybitAccountGatewayError::Readback),
        };
        pages.push(raw);
        if cursor.is_none() {
            return Ok(pages);
        }
    }
    Err(BybitAccountGatewayError::Readback)
}

fn recovery_outcome(
    command: &ExecutionCommand,
    readback: &BybitClosedOrderReadback,
) -> Result<AccountRecoveryOutcome, BybitAccountGatewayError> {
    let settlement = readback
        .exact_settlement()
        .map_err(|_| BybitAccountGatewayError::Readback)?;
    let Some(settlement) = settlement else {
        return Ok(AccountRecoveryOutcome::still_unknown(
            command.command_id().clone(),
        ));
    };
    if matches!(command, ExecutionCommand::Cancel(_)) {
        return Ok(if settlement.state == OrderState::Cancelled {
            AccountRecoveryOutcome::accepted(command.command_id().clone(), settlement.order_id)
        } else {
            AccountRecoveryOutcome::still_unknown(command.command_id().clone())
        });
    }
    Ok(if settlement.state == OrderState::Rejected {
        AccountRecoveryOutcome::rejected(
            command.command_id().clone(),
            "bybit_order_rejected".to_owned(),
        )
    } else {
        AccountRecoveryOutcome::accepted(command.command_id().clone(), settlement.order_id)
    })
}

#[derive(Debug, thiserror::Error)]
pub enum BybitAccountGatewayError {
    #[error("Bybit account gateway binding is invalid")]
    Binding,
    #[error("Bybit account gateway credentials are unavailable")]
    Credentials,
    #[error("Bybit account gateway runtime could not be created")]
    Runtime,
    #[error("Bybit account gateway attempt identity overflowed")]
    Attempt,
    #[error("Bybit public instrument rules are unavailable or invalid")]
    Instrument,
    #[error("Bybit API key lacks the required order and position permissions")]
    Permissions,
    #[error("Bybit signed account identity response is invalid")]
    AccountIdentity,
    #[error("Bybit account is not UTA2/UTA2 Pro")]
    AccountMode,
    #[error("Bybit DOGE position response does not prove both hedge legs")]
    Positions,
    #[error("Bybit DOGE position payload failed validation at {0}")]
    PositionPayload(&'static str),
    #[error("Bybit DOGE contract is in one-way mode; hedge mode is required")]
    PositionMode,
    #[error("Bybit exact signed order readback failed")]
    Readback,
    #[error("Bybit transport setup failed")]
    Transport(#[source] BybitTransportError),
    #[error("Bybit public instrument request failed")]
    PublicTransport(#[source] BybitTransportError),
    #[error("Bybit signed API-key request failed")]
    ApiKeyTransport(#[source] BybitTransportError),
    #[error("Bybit signed account request failed")]
    AccountTransport(#[source] BybitTransportError),
    #[error("Bybit signed position request failed")]
    PositionTransport(#[source] BybitTransportError),
    #[error("Bybit signed order readback request failed")]
    OrderTransport(#[source] BybitTransportError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use venue_domain::domain::{CommandId, OrderOwner, OrderPurpose, Position};
    use venue_gateway_api::{GatewayMode, VenueId};

    const INSTRUMENT: &str = include_str!("../fixtures/instruments-linear.json");
    const BBO: &str = include_str!("../fixtures/orderbook-linear-bbo.json");
    const EXECUTIONS: &[u8] = include_bytes!("../fixtures/execution-trade-page.json");

    fn limit_facts() -> Result<
        (
            BybitGatewayBinding,
            BybitLinearInstrumentRules,
            crate::BybitRestBbo,
        ),
        Box<dyn std::error::Error>,
    > {
        let binding = BybitGatewayBinding::new(GatewayBinding::new(
            VenueId::Bybit,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDT".parse()?,
        )?)?;
        let rules = parse_linear_instrument(
            &binding,
            crate::BybitRawPublicPayload::new(
                &binding,
                crate::BybitPublicSource::LinearInstrument,
                7,
                10_000,
                INSTRUMENT.to_owned(),
            )?,
        )?;
        let bbo = parse_rest_bbo(
            &binding,
            crate::BybitRawPublicPayload::new(
                &binding,
                crate::BybitPublicSource::RestOrderBook,
                7,
                10_000,
                BBO.to_owned(),
            )?,
        )?;
        Ok((binding, rules, bbo))
    }

    fn limit_intent(
        quote_delta: Decimal,
    ) -> Result<AccountLimitNormalizationIntent, Box<dyn std::error::Error>> {
        Ok(AccountLimitNormalizationIntent {
            command_id: CommandId::new("bybit_limit")?,
            client_order_id: CommandId::new("bybit_limit_client")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "bybit".to_owned(),
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

    fn account_wide_execution(
        binding: &BybitGatewayBinding,
        page_index: u32,
        request_cursor: Option<&str>,
        payload: Vec<u8>,
    ) -> crate::BybitRawPrivatePayload {
        crate::BybitRawPrivatePayload {
            parser_schema_version: crate::BYBIT_PRIVATE_PARSER_SCHEMA_VERSION,
            binding: binding.gateway_binding().clone(),
            source: BybitPrivateSource::AccountWideExecutions,
            native_symbol: "BTCUSDT".to_owned(),
            generation: 7,
            attempt_id: 1,
            page_index,
            request_cursor: request_cursor.map(str::to_owned),
            history_window: Some(BybitHistoryWindow {
                start_ms: 1,
                end_ms: 2_100,
            }),
            lookup: None,
            request_path: "/v5/execution/list".to_owned(),
            request_query: "category=linear".to_owned(),
            request_timestamp_ms: 2_000,
            received_at_ms: 3_000,
            payload_sha256: "fixture".to_owned(),
            payload,
        }
    }

    #[test]
    fn production_profile_is_bounded() {
        assert_eq!(EXACT_READBACK_MAX_PAGES, 32);
        assert_eq!(
            HISTORY_WINDOW_MS,
            std::time::Duration::from_secs(7 * 24 * 60 * 60).as_millis() as u64
        );
    }

    fn reduce(quantity: Decimal) -> Result<MarketReduceCommand, Box<dyn std::error::Error>> {
        Ok(MarketReduceCommand {
            command_id: CommandId::new("bybit_reduce")?,
            client_order_id: CommandId::new("bybit_reduce_client")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "bybit".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDT".parse()?,
                purpose: OrderPurpose::ExposureTakeProfit,
            },
            position_side: PositionSide::Long,
            side: venue_domain::domain::OrderSide::Sell,
            quantity,
            risk_episode_id: CommandId::new("bybit_episode")?,
            position_generation: 3,
        })
    }

    #[test]
    fn market_reduce_never_crosses_or_uses_a_wrong_signed_hedge_leg()
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
        let mut wrong_leg = position;
        wrong_leg.side = PositionSide::Short;
        assert!(
            validate_market_reduce_against_position(&reduce(Decimal::ONE)?, &wrong_leg).is_err()
        );
        Ok(())
    }

    #[test]
    fn limit_normalization_uses_same_side_bbo_and_floors_price_and_quantity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules, bbo) = limit_facts()?;
        let intent = limit_intent(Decimal::new(100, 0))?;
        let ExecutionCommand::PlaceLimit(command) =
            normalize_limit_from_bbo(&binding, &rules, &intent, &bbo, 10_010)?
        else {
            return Err("expected limit".into());
        };
        assert_eq!(command.limit_price.value(), Decimal::new(654_854, 1));
        assert_eq!(command.quantity, Decimal::new(1, 3));
        assert_eq!(command.command_id, intent.command_id);
        assert_eq!(command.client_order_id, intent.client_order_id);
        assert_eq!(command.owner, intent.owner);
        Ok(())
    }

    #[test]
    fn limit_normalization_rejects_minimum_symbol_direction_and_stale_bbo()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, rules, bbo) = limit_facts()?;
        assert!(
            normalize_limit_from_bbo(
                &binding,
                &rules,
                &limit_intent(Decimal::new(4, 0))?,
                &bbo,
                10_010,
            )
            .is_err()
        );
        let mut wrong_symbol = limit_intent(Decimal::new(100, 0))?;
        wrong_symbol.owner.symbol = "ETH/USDT".parse()?;
        assert!(normalize_limit_from_bbo(&binding, &rules, &wrong_symbol, &bbo, 10_010).is_err());
        let mut wrong_account = limit_intent(Decimal::new(100, 0))?;
        wrong_account.owner.account = "00000000-0000-4000-8000-000000000002".to_owned();
        assert!(normalize_limit_from_bbo(&binding, &rules, &wrong_account, &bbo, 10_010).is_err());
        let mut wrong_leg = limit_intent(Decimal::new(100, 0))?;
        wrong_leg.position_side = PositionSide::Short;
        assert!(normalize_limit_from_bbo(&binding, &rules, &wrong_leg, &bbo, 10_010).is_err());
        assert!(
            normalize_limit_from_bbo(
                &binding,
                &rules,
                &limit_intent(Decimal::new(100, 0))?,
                &bbo,
                11_001,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn snapshot_fills_deduplicates_closed_account_wide_pages_and_keeps_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, _, _) = limit_facts()?;
        let first = String::from_utf8(EXECUTIONS.to_vec())?
            .replace("\"nextPageCursor\": \"\"", "\"nextPageCursor\": \"next\"");
        let (fills, cursor) = snapshot_fills(
            &[
                account_wide_execution(&binding, 0, None, first.into_bytes()),
                account_wide_execution(&binding, 1, Some("next"), EXECUTIONS.to_vec()),
            ],
            None,
        )?;
        assert_eq!(fills.len(), 3);
        assert_eq!(fills[0].execution_sequence, FieldState::Known(103));
        assert_eq!(fills[0].exchange_time_ms, Some(2_000));
        assert_eq!(cursor, "bybit-exec:2000:c");
        Ok(())
    }

    #[test]
    fn snapshot_fills_rejects_unclosed_or_unknown_account_symbols()
    -> Result<(), Box<dyn std::error::Error>> {
        let (binding, _, _) = limit_facts()?;
        let unclosed = String::from_utf8(EXECUTIONS.to_vec())?
            .replace("\"nextPageCursor\": \"\"", "\"nextPageCursor\": \"next\"");
        assert!(
            snapshot_fills(
                &[account_wide_execution(
                    &binding,
                    0,
                    None,
                    unclosed.into_bytes(),
                )],
                None
            )
            .is_err()
        );
        let wrong_symbol = String::from_utf8(EXECUTIONS.to_vec())?.replace("BTCUSDT", "BTCUSDC");
        assert!(
            snapshot_fills(
                &[account_wide_execution(
                    &binding,
                    0,
                    None,
                    wrong_symbol.into_bytes(),
                )],
                None
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn fills_cursor_restarts_with_overlap_and_rejects_missing_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let resumed = fills_history_window(1_000_000, Some("bybit-exec:990000:exec-9"))?;
        assert_eq!(resumed.start_ms, 930_000);
        assert!(fills_history_window(HISTORY_WINDOW_MS + 2, Some("bybit-exec:1:exec-1")).is_err());
        assert!(fills_history_window(1_000_000, Some("okx-bill:99")).is_err());
        Ok(())
    }
}
