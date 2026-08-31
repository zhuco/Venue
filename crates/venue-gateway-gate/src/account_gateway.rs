use crate::{
    GATE_PRIVATE_PAGE_LIMIT, GATE_STAGE7_ORDER_PROFILE_VERSION, GateContractRules, GateCredentials,
    GateFillsCursor, GateGatewayBinding, GateHttpTransport, GateMutationDispatch,
    GatePrivateReadSource, GatePrivateReadbackCandidate, GatePublicBinding, GatePublicPayloadKind,
    GatePublicRawPayload, GateRawPrivateResponse, GateTransportError, GateTransportLimits,
    canonical_client_id_from_native, endpoints, parse_contract_rules,
    parse_rest_snapshot, prepare_cancel, prepare_exact_readback_by_client_id, prepare_limit,
    prepare_private_read, prepare_reduce_once, rest_order_book_path, settle_exact_readback,
    validate_private_readback,
};
use rust_decimal::Decimal;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{
    ExecutionCommand, FieldState, Fill, LimitTimeInForce, NativeOrderFamily, Order, OrderCommand,
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

#[path = "account_gateway_priced.rs"]
mod account_gateway_priced;
use account_gateway_priced::normalize_priced_limit;

const LIMIT_BBO_MAX_AGE_MS: u64 = 3_000;
const LIMIT_BBO_DEPTH: u16 = 20;

/// Production Gate adapter for the account host. The only mutation call consumes the host's
/// linear permit; all account-wide risk reads remain signed GET requests inside this crate.
pub struct GateAccountGateway {
    runtime: Runtime,
    binding: GateGatewayBinding,
    credentials: GateCredentials,
    transport: GateHttpTransport,
    rules: GateContractRules,
    rules_catalog: BTreeMap<Symbol, GateContractRules>,
    private: GatePrivateReadbackCandidate,
    next_attempt: u64,
}

impl GateAccountGateway {
    pub fn connect_from_environment(
        binding: GatewayBinding,
        limits: GateTransportLimits,
    ) -> Result<Self, GateAccountGatewayError> {
        let binding =
            GateGatewayBinding::new(binding).map_err(|_| GateAccountGatewayError::Binding)?;
        let credentials = GateCredentials::from_environment()
            .map_err(|_| GateAccountGatewayError::Credentials)?;
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| GateAccountGatewayError::Runtime)?;
        let generation = now_ms()?;
        let transport = GateHttpTransport::new(&binding, generation, limits)
            .map_err(GateAccountGatewayError::Transport)?;
        let rules = runtime.block_on(fetch_selected_rules(&transport, &binding, generation))?;
        let private =
            runtime.block_on(fetch_private(&transport, &binding, &credentials, &rules, 1))?;
        let rules_catalog = BTreeMap::from([(rules.instrument.symbol.clone(), rules.clone())]);
        Ok(Self {
            runtime,
            binding,
            credentials,
            transport,
            rules,
            rules_catalog,
            private,
            next_attempt: 2,
        })
    }

    fn next_attempt(&mut self) -> Result<u64, GateAccountGatewayError> {
        let attempt = self.next_attempt;
        self.next_attempt = self
            .next_attempt
            .checked_add(1)
            .ok_or(GateAccountGatewayError::Attempt)?;
        Ok(attempt)
    }

    fn refresh_rules_for_symbols<I>(&mut self, symbols: I) -> Result<(), GateAccountGatewayError>
    where
        I: IntoIterator<Item = Symbol>,
    {
        let mut required = self.rules_catalog.keys().cloned().collect::<BTreeSet<_>>();
        required.extend(symbols);
        required.insert(self.binding.gateway_binding().symbol.clone());
        let catalogue = self
            .runtime
            .block_on(self.transport.fetch_public_contracts())
            .map_err(GateAccountGatewayError::Transport)?;
        let current = catalogue_rules(&catalogue, self.rules.instrument.generation, required)?;
        if self
            .rules_catalog
            .iter()
            .any(|(symbol, previous)| current.get(symbol) != Some(previous))
        {
            return Err(GateAccountGatewayError::RulesChanged);
        }
        self.rules_catalog = current;
        self.rules = catalog_rule(&self.rules_catalog, &self.binding.gateway_binding().symbol)?;
        Ok(())
    }

    fn registered_rules(
        &self,
        symbol: &Symbol,
    ) -> Result<GateContractRules, GateAccountGatewayError> {
        catalog_rule(&self.rules_catalog, symbol)
    }

    fn refresh_private_for(&mut self, symbol: &Symbol) -> Result<(), GateAccountGatewayError> {
        self.refresh_rules_for_symbols([symbol.clone()])?;
        let rules = self.registered_rules(symbol)?;
        let attempt = self.next_attempt()?;
        self.private = self.runtime.block_on(fetch_private(
            &self.transport,
            &self.binding,
            &self.credentials,
            &rules,
            attempt,
        ))?;
        Ok(())
    }

    fn dispatch_permit(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        if permit.binding() != self.binding.gateway_binding() {
            return rejected("gate_permit_binding");
        }
        let symbol = permit.command().mutation_owner().symbol.clone();
        if self.refresh_private_for(&symbol).is_err() {
            return rejected("gate_preflight_failed");
        }
        let rules = match self.registered_rules(&symbol) {
            Ok(value) => value,
            Err(_) => return rejected("gate_symbol_unconfigured"),
        };
        let prepared = match permit.command() {
            ExecutionCommand::PlaceLimit(command) => prepare_limit(&self.binding, &rules, command),
            ExecutionCommand::MarketReduce(command) => {
                prepare_reduce_once(&self.binding, &rules, command)
            }
            ExecutionCommand::Cancel(command) => {
                let target = regular_venue_order_id_for_client_id(
                    &self.private.order_families.regular().orders,
                    command.target_client_order_id.as_str(),
                );
                match target {
                    Some(venue_order_id) => prepare_cancel(
                        &self.binding,
                        &rules,
                        &crate::GateCancelIntent {
                            command: command.clone(),
                            venue_order_id,
                        },
                    ),
                    None => return rejected("gate_cancel_target_unproven"),
                }
            }
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                return rejected("gate_command_unsupported");
            }
        };
        let prepared = match prepared {
            Ok(value) => value,
            Err(_) => return rejected("gate_intent_rejected"),
        };
        match self.runtime.block_on(self.transport.execute_mutation(
            &self.binding,
            &self.credentials,
            &rules,
            prepared,
            match now_ms() {
                Ok(value) => value,
                Err(_) => return rejected("gate_clock"),
            },
        )) {
            Ok(GateMutationDispatch::Accepted(accepted)) => {
                self.settle_exact(&rules, &accepted.readback)
            }
            Ok(GateMutationDispatch::Unknown(unknown)) => {
                self.settle_exact(&rules, &unknown.readback)
            }
            Err(GateTransportError::VenueRejected) => rejected("gate_venue_rejected"),
            Err(_) => AccountGatewayResult::Unknown,
        }
    }

    fn settle_exact(
        &mut self,
        rules: &GateContractRules,
        request: &crate::GateExactReadbackRequest,
    ) -> AccountGatewayResult {
        match self.runtime.block_on(self.transport.execute_exact_readback(
            &self.binding,
            &self.credentials,
            rules,
            request,
            match now_ms() {
                Ok(value) => value,
                Err(_) => return AccountGatewayResult::Unknown,
            },
        )) {
            Ok(readback) => match settle_exact_readback(request, &readback) {
                Ok(settlement) => AccountGatewayResult::Accepted {
                    venue_order_id: settlement.order.order_id,
                },
                Err(_) => AccountGatewayResult::Unknown,
            },
            Err(_) => AccountGatewayResult::Unknown,
        }
    }

    fn normalize_limit(
        &mut self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        intent.validate()?;
        let binding = self.binding.gateway_binding();
        if intent.owner.exchange != binding.venue.as_str()
            || intent.owner.account != binding.trading_account_id
        {
            return Err(AccountHostValidationError::Scope);
        }
        self.refresh_rules_for_symbols([intent.owner.symbol.clone()])
            .map_err(|_| AccountHostValidationError::Command)?;
        let rules = self
            .registered_rules(&intent.owner.symbol)
            .map_err(|_| AccountHostValidationError::Scope)?;
        let now = now_ms().map_err(|_| AccountHostValidationError::Command)?;
        let (bid, ask) = self.runtime.block_on(fetch_fresh_limit_bbo(
            &self.transport,
            &self.binding,
            &rules,
            now,
        ))?;
        normalize_limit_from_bbo(intent, &rules, bid, ask)
    }
}

/// Gate's parser removes the exchange-only `t-` transport prefix before exposing a client id.
/// A Host cancel carries that canonical durable id, so comparing it to a freshly re-prefixed id
/// would reject every exact owned cancel and strand a rolling grid's replacement transaction.
fn regular_venue_order_id_for_client_id(
    orders: &[Order],
    target_client_id: &str,
) -> Option<String> {
    orders
        .iter()
        .find(|order| {
            matches!(&order.client_order_id, FieldState::Known(value) if value == target_client_id)
        })
        .map(|order| order.order_id.clone())
}

impl AccountPhysicalGateway for GateAccountGateway {
    type Error = GateAccountGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.binding.gateway_binding()
    }

    fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        self.current_instrument_for(&self.binding.gateway_binding().symbol.clone())
    }

    fn current_instrument_for(
        &mut self,
        symbol: &Symbol,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        // Only a signed recovery/snapshot request registers non-anchor symbols.  A caller cannot
        // turn an arbitrary canonical symbol into a dispatchable Gate contract by probing rules.
        if !self.rules_catalog.contains_key(symbol) {
            return Err(AccountHostValidationError::Instrument);
        }
        self.refresh_rules_for_symbols(std::iter::empty())
            .map_err(|_| AccountHostValidationError::Instrument)?;
        let current = self
            .registered_rules(symbol)
            .map_err(|_| AccountHostValidationError::Instrument)?;
        current
            .instrument
            .validate()
            .map_err(|_| AccountHostValidationError::Instrument)?;
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
            return Err(GateAccountGatewayError::Binding);
        }
        self.refresh_rules_for_symbols(request.configured_symbols().iter().cloned())?;
        let observed_at_ms = now_ms()?;
        let mut outcomes = Vec::with_capacity(request.unresolved().len());
        for command in request.unresolved() {
            let client_id = match command {
                ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
                _ => command
                    .native_client_id()
                    .ok_or(GateAccountGatewayError::Readback)?
                    .as_str(),
            };
            let rules = self.registered_rules(&command.mutation_owner().symbol)?;
            let outcome =
                match prepare_exact_readback_by_client_id(&self.binding, &rules, client_id)
                    .ok()
                    .and_then(|exact| {
                        now_ms().ok().and_then(|timestamp| {
                            self.runtime
                                .block_on(self.transport.execute_exact_readback(
                                    &self.binding,
                                    &self.credentials,
                                    &rules,
                                    &exact,
                                    timestamp,
                                ))
                                .ok()
                        })
                    }) {
                    Some(readback)
                        if readback_policy_matches_command(command, &readback.order)
                            && readback.order.state == OrderState::Rejected =>
                    {
                        AccountRecoveryOutcome::rejected(
                            command.command_id().clone(),
                            "gate_rejected".to_owned(),
                        )
                    }
                    Some(readback) if readback_policy_matches_command(command, &readback.order) => {
                        AccountRecoveryOutcome::accepted(
                            command.command_id().clone(),
                            readback.order.order_id,
                        )
                    }
                    Some(_) => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
                    None => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
                };
            outcomes.push(outcome);
        }
        AccountRecoveryReport::new(
            self.binding.gateway_binding().clone(),
            observed_at_ms,
            outcomes,
        )
        .map_err(|_| GateAccountGatewayError::Readback)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        self.refresh_rules_for_symbols(std::iter::empty())
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let attempt = self
            .next_attempt()
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        self.runtime.block_on(fetch_account_wide_risk(
            &self.transport,
            &self.binding,
            &self.credentials,
            &self.rules,
            attempt,
        ))
    }

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        if request.binding() != self.binding.gateway_binding() {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        self.refresh_rules_for_symbols(request.configured_symbols().iter().cloned())
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let attempt = self
            .next_attempt()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        self.runtime.block_on(fetch_account_wide_snapshot(
            &self.transport,
            &self.binding,
            &self.credentials,
            &self.rules,
            &self.rules_catalog,
            attempt,
            request,
        ))
    }

    fn normalize_limit_intent(
        &mut self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        self.normalize_limit(intent)
    }

    fn normalize_priced_limit_intent(
        &mut self,
        intent: &AccountPricedLimitIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        intent.validate()?;
        let binding = self.binding.gateway_binding();
        if intent.intent.owner.exchange != binding.venue.as_str()
            || intent.intent.owner.account != binding.trading_account_id
        {
            return Err(AccountHostValidationError::Scope);
        }
        self.refresh_rules_for_symbols([intent.intent.owner.symbol.clone()])
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

async fn fetch_fresh_limit_bbo(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    rules: &GateContractRules,
    requested_at_ms: u64,
) -> Result<(Decimal, Decimal), AccountHostValidationError> {
    if requested_at_ms == 0
        || rules.instrument.symbol != binding.gateway_binding().symbol
        || rules.native_symbol.is_empty()
    {
        return Err(AccountHostValidationError::Command);
    }
    let public_binding = GatePublicBinding::new(
        rules.instrument.symbol.clone(),
        rules.native_symbol.clone(),
        rules.quanto_multiplier,
    )
    .map_err(|_| AccountHostValidationError::Command)?;
    let path = rest_order_book_path(&public_binding, LIMIT_BBO_DEPTH)
        .map_err(|_| AccountHostValidationError::Command)?;
    let payload = transport
        .fetch_public_order_book(&path)
        .await
        .map_err(|_| AccountHostValidationError::Command)?;
    let received_at_ms = now_ms().map_err(|_| AccountHostValidationError::Command)?;
    parse_fresh_limit_bbo(
        rules,
        public_binding,
        payload,
        requested_at_ms,
        received_at_ms,
    )
}

fn parse_fresh_limit_bbo(
    rules: &GateContractRules,
    public_binding: GatePublicBinding,
    payload: String,
    requested_at_ms: u64,
    received_at_ms: u64,
) -> Result<(Decimal, Decimal), AccountHostValidationError> {
    if requested_at_ms == 0 || received_at_ms < requested_at_ms {
        return Err(AccountHostValidationError::Command);
    }
    let raw = GatePublicRawPayload::new(
        &public_binding,
        GatePublicPayloadKind::RestOrderBookSnapshot,
        rules.instrument.generation,
        received_at_ms,
        payload,
    )
    .map_err(|_| AccountHostValidationError::Command)?;
    let snapshot = parse_rest_snapshot(&public_binding, raw)
        .map_err(|_| AccountHostValidationError::Command)?;
    let exchange_time_ms = snapshot
        .value
        .exchange_time_ms
        .ok_or(AccountHostValidationError::Command)?;
    if received_at_ms < requested_at_ms
        || received_at_ms < exchange_time_ms
        || received_at_ms.saturating_sub(exchange_time_ms) > LIMIT_BBO_MAX_AGE_MS
    {
        return Err(AccountHostValidationError::Command);
    }
    let bid = snapshot
        .value
        .bids
        .first()
        .map(|level| level.price.value())
        .filter(|price| *price > Decimal::ZERO)
        .ok_or(AccountHostValidationError::Command)?;
    let ask = snapshot
        .value
        .asks
        .first()
        .map(|level| level.price.value())
        .filter(|price| *price > Decimal::ZERO)
        .ok_or(AccountHostValidationError::Command)?;
    if bid >= ask {
        return Err(AccountHostValidationError::Command);
    }
    Ok((bid, ask))
}

fn normalize_limit_from_bbo(
    intent: &AccountLimitNormalizationIntent,
    rules: &GateContractRules,
    bid: Decimal,
    ask: Decimal,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    if intent.owner.symbol != rules.instrument.symbol
        || bid <= Decimal::ZERO
        || ask <= Decimal::ZERO
        || bid >= ask
    {
        return Err(AccountHostValidationError::Command);
    }
    let price = match intent.side {
        OrderSide::Buy => bid,
        OrderSide::Sell => ask,
    };
    if price % rules.instrument.price_tick.value() != Decimal::ZERO {
        return Err(AccountHostValidationError::Command);
    }
    let quantity = intent
        .quote_delta
        .checked_div(price)
        .map(|value| value - value % rules.quanto_multiplier)
        .filter(|value| *value > Decimal::ZERO && *value >= rules.minimum_quantity())
        .ok_or(AccountHostValidationError::Command)?;
    rules
        .native_order_contracts_checked(quantity)
        .map_err(|_| AccountHostValidationError::Command)?;
    let command = OrderCommand {
        time_in_force: Default::default(),
        command_id: intent.command_id.clone(),
        client_order_id: intent.client_order_id.clone(),
        owner: intent.owner.clone(),
        side: intent.side,
        position_side: intent.position_side,
        quantity,
        limit_price: Price::new(price).map_err(|_| AccountHostValidationError::Command)?,
        reduce_only: intent.reduce_only,
    };
    command
        .validate()
        .map_err(|_| AccountHostValidationError::Command)?;
    Ok(ExecutionCommand::PlaceLimit(command))
}

async fn fetch_selected_rules(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    generation: u64,
) -> Result<GateContractRules, GateAccountGatewayError> {
    let catalogue = transport
        .fetch_public_contracts()
        .await
        .map_err(GateAccountGatewayError::Transport)?;
    contract_rules(
        &catalogue,
        binding.gateway_binding().symbol.clone(),
        generation,
    )
    .map_err(|_| GateAccountGatewayError::Rules)
}

fn catalogue_rules(
    catalogue: &str,
    generation: u64,
    symbols: BTreeSet<Symbol>,
) -> Result<BTreeMap<Symbol, GateContractRules>, GateAccountGatewayError> {
    if generation == 0 || symbols.is_empty() {
        return Err(GateAccountGatewayError::Rules);
    }
    let mut rules = BTreeMap::new();
    for symbol in symbols {
        let rule = contract_rules(catalogue, symbol.clone(), generation)
            .map_err(|_| GateAccountGatewayError::Rules)?;
        if rules.insert(symbol, rule).is_some() {
            return Err(GateAccountGatewayError::Rules);
        }
    }
    Ok(rules)
}

fn catalog_rule(
    catalog: &BTreeMap<Symbol, GateContractRules>,
    symbol: &Symbol,
) -> Result<GateContractRules, GateAccountGatewayError> {
    catalog
        .get(symbol)
        .cloned()
        .ok_or(GateAccountGatewayError::Rules)
}

async fn fetch_private(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    attempt: u64,
) -> Result<GatePrivateReadbackCandidate, GateAccountGatewayError> {
    let started_at_ms = now_ms()?;
    let deadline = started_at_ms
        .checked_add(3_000)
        .ok_or(GateAccountGatewayError::Clock)?;
    let mut responses = Vec::new();
    for source in [
        GatePrivateReadSource::Account,
        GatePrivateReadSource::DualPositions,
    ] {
        responses.push(
            fetch_private_page(
                transport,
                binding,
                credentials,
                rules,
                attempt,
                source,
                GateFillsCursor::default(),
            )
            .await?,
        );
    }
    for source in [
        GatePrivateReadSource::RegularOrders,
        GatePrivateReadSource::Fills,
    ] {
        let mut cursor = GateFillsCursor::default();
        for _ in 0..crate::GATE_PRIVATE_MAX_PAGES {
            let response = fetch_private_page(
                transport,
                binding,
                credentials,
                rules,
                attempt,
                source,
                cursor.clone(),
            )
            .await?;
            let next = page_cursor(&response.payload)?;
            let terminal = next.1;
            responses.push(response);
            if terminal {
                break;
            }
            cursor = GateFillsCursor::new(next.0).map_err(|_| GateAccountGatewayError::Readback)?;
        }
    }
    validate_private_readback(
        binding,
        rules,
        GATE_STAGE7_ORDER_PROFILE_VERSION,
        deadline,
        now_ms()?,
        responses,
    )
    .map_err(|_| GateAccountGatewayError::Readback)
}

async fn fetch_private_page(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    attempt: u64,
    source: GatePrivateReadSource,
    cursor: GateFillsCursor,
) -> Result<GateRawPrivateResponse, GateAccountGatewayError> {
    let request = prepare_private_read(
        binding,
        rules,
        rules.instrument.generation,
        attempt,
        source,
        cursor,
    )
    .map_err(|_| GateAccountGatewayError::Readback)?;
    transport
        .execute_private_read(binding, credentials, rules, &request, now_ms()?)
        .await
        .map_err(GateAccountGatewayError::Transport)
}

/// The Gate profile exposes one regular open-order namespace. Conditional and Algo families are
/// explicitly unsupported by the immutable profile evidence, so a snapshot must close exactly
/// this paged regular family plus the account-wide dual positions and fills surfaces.
async fn fetch_account_wide_snapshot(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    selected_rules: &GateContractRules,
    rules_catalog: &BTreeMap<Symbol, GateContractRules>,
    attempt: u64,
    recovery: &AccountRecoveryRequest,
) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
    if attempt == 0 {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let observed_at_ms = now_ms().map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let catalogue = transport
        .fetch_public_contracts()
        .await
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let account = snapshot_read(
        transport,
        binding,
        credentials,
        selected_rules,
        endpoints::FUTURES_ACCOUNT,
        "",
    )
    .await?;
    let account_value: Value =
        serde_json::from_str(&account).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    if !crate::parse_dual_position_mode(&account_value).is_ok() {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let positions = snapshot_read(
        transport,
        binding,
        credentials,
        selected_rules,
        endpoints::POSITIONS,
        "holding=false",
    )
    .await?;
    let positions: Vec<Value> =
        serde_json::from_str(&positions).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let (regular, regular_payloads) = snapshot_paged_rows(
        transport,
        binding,
        credentials,
        selected_rules,
        "status=open",
        endpoints::FUTURES_OPEN_ORDERS,
    )
    .await?;
    let previous_fills_cursor = parse_snapshot_fills_cursor(recovery.previous_fills_cursor())?;
    let (fills, fill_payloads) = snapshot_paged_rows_from_cursor(
        transport,
        binding,
        credentials,
        selected_rules,
        "",
        endpoints::FUTURES_FILLS,
        previous_fills_cursor.as_deref(),
    )
    .await?;
    let position_facts =
        snapshot_position_facts(&catalogue, &positions, selected_rules.instrument.generation)?;
    let order_facts =
        snapshot_regular_order_facts(&catalogue, &regular, selected_rules.instrument.generation)?;
    let unknown_results =
        snapshot_unknown_results(transport, binding, credentials, rules_catalog, recovery).await?;
    let fills_cursor = snapshot_fills_cursor(&fills, &fill_payloads, previous_fills_cursor)?;
    let fill_facts = snapshot_fill_facts(&catalogue, &fills, selected_rules.instrument.generation)?;
    if regular_payloads.is_empty() {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let balance = crate::private::parse_account_balance(&account_value)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    SignedAccountSnapshot::complete_with_fills(
        binding.gateway_binding().clone(),
        observed_at_ms,
        selected_rules.instrument.generation,
        attempt,
        selected_rules.instrument.generation,
        SignedAccountPositionMode::Hedge,
        order_facts,
        position_facts,
        fill_facts,
        fills_cursor,
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

async fn snapshot_read(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    endpoint: &str,
    query: &str,
) -> Result<String, AccountHostValidationError> {
    transport
        .execute_account_risk_read(
            binding,
            credentials,
            rules,
            endpoint,
            query,
            now_ms().map_err(|_| AccountHostValidationError::SignedSnapshot)?,
        )
        .await
        .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

async fn snapshot_paged_rows(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    fixed_query: &str,
    endpoint: &str,
) -> Result<(Vec<Value>, Vec<String>), AccountHostValidationError> {
    snapshot_paged_rows_from_cursor(
        transport,
        binding,
        credentials,
        rules,
        fixed_query,
        endpoint,
        None,
    )
    .await
}

async fn snapshot_paged_rows_from_cursor(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    fixed_query: &str,
    endpoint: &str,
    initial_last_id: Option<&str>,
) -> Result<(Vec<Value>, Vec<String>), AccountHostValidationError> {
    let mut all = Vec::new();
    let mut raw_pages = Vec::new();
    let mut seen_ids = BTreeSet::new();
    let mut last_id = initial_last_id.map(str::to_owned);
    for _ in 0..crate::GATE_PRIVATE_MAX_PAGES {
        let mut query = format!("limit={GATE_PRIVATE_PAGE_LIMIT}");
        if !fixed_query.is_empty() {
            query.push('&');
            query.push_str(fixed_query);
        }
        if let Some(cursor) = &last_id {
            query.push_str("&last_id=");
            query.push_str(cursor);
        }
        let payload =
            snapshot_read(transport, binding, credentials, rules, endpoint, &query).await?;
        let rows: Vec<Value> = serde_json::from_str(&payload)
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        if rows.len() > GATE_PRIVATE_PAGE_LIMIT {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let page_last = rows
            .last()
            .and_then(|row| row.get("id"))
            .and_then(value_id)
            .map(str::to_owned);
        for row in &rows {
            let id = row
                .get("id")
                .and_then(value_id)
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            if !seen_ids.insert(id.to_owned()) {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
        }
        let terminal = rows.len() < GATE_PRIVATE_PAGE_LIMIT;
        raw_pages.push(payload);
        all.extend(rows);
        if terminal {
            return Ok((all, raw_pages));
        }
        last_id = Some(page_last.ok_or(AccountHostValidationError::SignedSnapshot)?);
    }
    Err(AccountHostValidationError::SignedSnapshot)
}

fn snapshot_position_facts(
    catalogue: &str,
    rows: &[Value],
    generation: u64,
) -> Result<Vec<SignedAccountPositionFact>, AccountHostValidationError> {
    let mut seen = BTreeSet::new();
    let mut facts = Vec::with_capacity(rows.len());
    for row in rows {
        let item = row
            .as_object()
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        let symbol = snapshot_symbol(item.get("contract"))?;
        let rules = snapshot_contract_rules(catalogue, symbol.clone(), generation)?;
        let side = match item.get("mode").and_then(Value::as_str) {
            Some("dual_long") => PositionSide::Long,
            Some("dual_short") => PositionSide::Short,
            _ => return Err(AccountHostValidationError::SignedSnapshot),
        };
        if !seen.insert((symbol.clone(), side)) {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let quantity = snapshot_decimal(item.get("size"))?
            .abs()
            .checked_mul(rules.quanto_multiplier)
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        facts.push(SignedAccountPositionFact {
            symbol,
            position_side: side,
            quantity,
            entry_price: snapshot_optional_price(item.get("entry_price"))?,
            mark_price: snapshot_optional_price(item.get("mark_price"))?,
        });
    }
    Ok(facts)
}

fn snapshot_regular_order_facts(
    catalogue: &str,
    rows: &[Value],
    generation: u64,
) -> Result<Vec<SignedAccountOrderFact>, AccountHostValidationError> {
    let mut ids = BTreeSet::new();
    rows.iter()
        .map(|row| {
            let item = row
                .as_object()
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            if item.get("status").and_then(Value::as_str) != Some("open") {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            let symbol = snapshot_symbol(item.get("contract"))?;
            let rules = snapshot_contract_rules(catalogue, symbol.clone(), generation)?;
            let signed_size = snapshot_decimal(item.get("size"))?;
            let left = snapshot_decimal(item.get("left"))?;
            let quantity = signed_size
                .abs()
                .checked_mul(rules.quanto_multiplier)
                .filter(|value| *value > Decimal::ZERO)
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            if signed_size.is_zero()
                || left.is_zero()
                || left.abs() > signed_size.abs()
                || quantity <= Decimal::ZERO
            {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            let native_client_order_id = snapshot_text(item.get("text"))?;
            // Only the exact `t-{canonical}` encoding is normalized. Any other native text is
            // retained as external and cannot become owned merely because it shares a prefix.
            let client_order_id = canonical_client_id_from_native(native_client_order_id)
                .unwrap_or_else(|| native_client_order_id.to_owned());
            if !ids.insert(client_order_id.clone()) {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            let side = if signed_size.is_sign_positive() {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let reduce_only = item
                .get("is_reduce_only")
                .and_then(Value::as_bool)
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            let position_side = match (reduce_only, side) {
                (false, OrderSide::Buy) | (true, OrderSide::Sell) => PositionSide::Long,
                (false, OrderSide::Sell) | (true, OrderSide::Buy) => PositionSide::Short,
            };
            Ok(SignedAccountOrderFact {
                client_order_id,
                venue_order_id: Some(snapshot_id(item.get("id"))?),
                symbol,
                family: NativeOrderFamily::UmOrder,
                side,
                position_side,
                quantity,
                limit_price: snapshot_optional_price(item.get("price"))?,
                time_in_force: snapshot_limit_time_in_force(item.get("tif"))?,
                created_at_ms: snapshot_created_at_ms(item.get("create_time"))?,
                reduce_only,
                owner: None,
                external: true,
                state: Some(if left.abs() == signed_size.abs() {
                    OrderState::New
                } else {
                    OrderState::PartiallyFilled
                }),
                filled_quantity: Some(
                    signed_size
                        .abs()
                        .checked_sub(left.abs())
                        .and_then(|value| value.checked_mul(rules.quanto_multiplier))
                        .ok_or(AccountHostValidationError::SignedSnapshot)?,
                ),
            })
        })
        .collect()
}

fn snapshot_fill_facts(
    catalogue: &str,
    rows: &[Value],
    generation: u64,
) -> Result<Vec<Fill>, AccountHostValidationError> {
    let mut ids = BTreeSet::new();
    rows.iter()
        .map(|row| {
            let item = row
                .as_object()
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            let fill_id = snapshot_id(item.get("id"))?;
            if !ids.insert(fill_id.clone()) {
                return Err(AccountHostValidationError::SignedSnapshot);
            }
            let symbol = snapshot_symbol(item.get("contract"))?;
            let rules = snapshot_contract_rules(catalogue, symbol.clone(), generation)?;
            let signed_size = snapshot_decimal(item.get("size"))?;
            let quantity = signed_size
                .abs()
                .checked_mul(rules.quanto_multiplier)
                .filter(|value| *value > Decimal::ZERO)
                .ok_or(AccountHostValidationError::SignedSnapshot)?;
            let price = Price::new(snapshot_decimal(item.get("price"))?)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let side = if signed_size.is_sign_positive() {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let sequence = fill_id
                .parse()
                .map(FieldState::Known)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            Ok(Fill {
                fill_id,
                execution_sequence: sequence,
                order_id: snapshot_id(item.get("order_id"))?,
                symbol,
                side,
                position_side: FieldState::Missing,
                quantity,
                price,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Missing,
                exchange_time_ms: None,
            })
        })
        .collect()
}

async fn snapshot_unknown_results(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules_catalog: &BTreeMap<Symbol, GateContractRules>,
    recovery: &AccountRecoveryRequest,
) -> Result<Vec<SignedUnknownFact>, AccountHostValidationError> {
    let mut results = Vec::with_capacity(recovery.unresolved().len());
    for command in recovery.unresolved() {
        let identity = match command {
            ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
            _ => command
                .native_client_id()
                .ok_or(AccountHostValidationError::SignedSnapshot)?
                .as_str(),
        };
        let rules = catalog_rule(rules_catalog, &command.mutation_owner().symbol)
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let result = match prepare_exact_readback_by_client_id(binding, &rules, identity) {
            Ok(exact) => match now_ms().ok() {
                Some(timestamp) => match transport
                    .execute_exact_readback(binding, credentials, &rules, &exact, timestamp)
                    .await
                {
                    Ok(readback)
                        if readback_policy_matches_command(command, &readback.order)
                            && readback.order.state == OrderState::Rejected =>
                    {
                        SignedUnknownResult::Rejected {
                            reason: "gate_rejected".to_owned(),
                        }
                    }
                    Ok(readback) if readback_policy_matches_command(command, &readback.order) => {
                        SignedUnknownResult::Accepted {
                            venue_order_id: readback.order.order_id,
                        }
                    }
                    Ok(_) => SignedUnknownResult::Unknown,
                    Err(_) => SignedUnknownResult::Unknown,
                },
                None => SignedUnknownResult::Unknown,
            },
            Err(_) => SignedUnknownResult::Unknown,
        };
        results.push(SignedUnknownFact {
            command_id: command.command_id().clone(),
            result,
        });
    }
    Ok(results)
}

fn readback_policy_matches_command(
    command: &ExecutionCommand,
    order: &venue_domain::domain::Order,
) -> bool {
    command_matches_readback_order(command, order)
}

fn snapshot_fills_cursor(
    fills: &[Value],
    raw_pages: &[String],
    previous: Option<String>,
) -> Result<String, AccountHostValidationError> {
    if raw_pages.is_empty() {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let mut ids = BTreeSet::new();
    for row in fills {
        let id = row
            .get("id")
            .and_then(value_id)
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        if !ids.insert(id.to_owned()) {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
    }
    let current = fills
        .last()
        .and_then(|row| row.get("id"))
        .and_then(value_id);
    let watermark = current
        .or(previous.as_deref())
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    if let Some(previous) = previous.as_deref() {
        let old = previous
            .parse::<u128>()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        let new = watermark
            .parse::<u128>()
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
        if new < old {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
    }
    // Gate's signed my_trades cursor is the native trade id.  A payload digest cannot resume a
    // page after restart and is therefore deliberately not persisted as a cursor.
    Ok(format!("gate-fills-v1|{watermark}"))
}

fn parse_snapshot_fills_cursor(
    value: Option<&str>,
) -> Result<Option<String>, AccountHostValidationError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let native = value
        .strip_prefix("gate-fills-v1|")
        // Legacy SHA cursors cannot identify the next Gate page; fail closed rather than start
        // from a recent window that could omit a restart gap.
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    Ok(Some(native.to_owned()))
}

fn snapshot_contract_rules(
    catalogue: &str,
    symbol: Symbol,
    generation: u64,
) -> Result<GateContractRules, AccountHostValidationError> {
    contract_rules(catalogue, symbol, generation)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

fn snapshot_symbol(value: Option<&Value>) -> Result<Symbol, AccountHostValidationError> {
    let native = snapshot_text(value)?;
    let base = native
        .strip_suffix("_USDT")
        .filter(|base| !base.is_empty())
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    format!("{base}/USDT")
        .parse()
        .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

fn snapshot_text(value: Option<&Value>) -> Result<&str, AccountHostValidationError> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(AccountHostValidationError::SignedSnapshot)
}

fn snapshot_id(value: Option<&Value>) -> Result<String, AccountHostValidationError> {
    match value {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(value.clone()),
        Some(Value::Number(value)) if value.as_u64().is_some() => Ok(value.to_string()),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn snapshot_decimal(value: Option<&Value>) -> Result<Decimal, AccountHostValidationError> {
    decimal_value(value).map_err(|_| AccountHostValidationError::SignedSnapshot)
}

fn snapshot_optional_price(
    value: Option<&Value>,
) -> Result<Option<Decimal>, AccountHostValidationError> {
    let value = snapshot_decimal(value)?;
    if value.is_zero() {
        Ok(None)
    } else if value.is_sign_positive() {
        Ok(Some(value))
    } else {
        Err(AccountHostValidationError::SignedSnapshot)
    }
}

fn snapshot_limit_time_in_force(
    value: Option<&Value>,
) -> Result<Option<LimitTimeInForce>, AccountHostValidationError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => match value.as_str() {
            "poc" => Ok(Some(LimitTimeInForce::PostOnly)),
            "gtc" => Ok(Some(LimitTimeInForce::Gtc)),
            _ => Ok(None),
        },
        Some(_) => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn snapshot_created_at_ms(
    value: Option<&Value>,
) -> Result<Option<u64>, AccountHostValidationError> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    let seconds = snapshot_decimal(Some(value))?;
    let milliseconds = seconds
        .checked_mul(Decimal::from(1_000))
        .filter(|value| *value > Decimal::ZERO && value.fract().is_zero())
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    milliseconds
        .normalize()
        .to_string()
        .parse::<u64>()
        .map(Some)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

async fn fetch_account_wide_risk(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    selected_rules: &GateContractRules,
    attempt: u64,
) -> Result<AccountRiskEvidence, AccountHostValidationError> {
    let observed_at_ms = now_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let catalogue = transport
        .fetch_public_contracts()
        .await
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let account = risk_read(
        transport,
        binding,
        credentials,
        selected_rules,
        endpoints::FUTURES_ACCOUNT,
        "",
    )
    .await?;
    let positions = risk_read(
        transport,
        binding,
        credentials,
        selected_rules,
        endpoints::POSITIONS,
        "holding=false",
    )
    .await?;
    let orders = risk_order_pages(transport, binding, credentials, selected_rules).await?;
    let account_value: Value =
        serde_json::from_str(&account).map_err(|_| AccountHostValidationError::RiskEvidence)?;
    if !crate::parse_dual_position_mode(&account_value).is_ok() || attempt == 0 {
        return Err(AccountHostValidationError::RiskEvidence);
    }
    let positions =
        account_position_notionals(&catalogue, &positions, selected_rules.instrument.generation)?;
    let orders =
        account_entry_order_notionals(&catalogue, &orders, selected_rules.instrument.generation)?;
    AccountRiskEvidence::complete(
        binding.gateway_binding().clone(),
        observed_at_ms,
        selected_rules.instrument.generation,
        positions,
        orders,
    )
}

async fn risk_read(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
    endpoint: &str,
    query: &str,
) -> Result<String, AccountHostValidationError> {
    transport
        .execute_account_risk_read(
            binding,
            credentials,
            rules,
            endpoint,
            query,
            now_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?,
        )
        .await
        .map_err(|_| AccountHostValidationError::RiskEvidence)
}

async fn risk_order_pages(
    transport: &GateHttpTransport,
    binding: &GateGatewayBinding,
    credentials: &GateCredentials,
    rules: &GateContractRules,
) -> Result<Vec<Value>, AccountHostValidationError> {
    let mut rows = Vec::new();
    let mut last_id: Option<String> = None;
    for _ in 0..crate::GATE_PRIVATE_MAX_PAGES {
        let mut query = format!("status=open&limit={GATE_PRIVATE_PAGE_LIMIT}");
        if let Some(cursor) = &last_id {
            query.push_str("&last_id=");
            query.push_str(cursor);
        }
        let payload = risk_read(
            transport,
            binding,
            credentials,
            rules,
            endpoints::FUTURES_OPEN_ORDERS,
            &query,
        )
        .await?;
        let page: Vec<Value> =
            serde_json::from_str(&payload).map_err(|_| AccountHostValidationError::RiskEvidence)?;
        if page.len() > GATE_PRIVATE_PAGE_LIMIT {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        if page.len() < GATE_PRIVATE_PAGE_LIMIT {
            rows.extend(page);
            return Ok(rows);
        }
        last_id = page
            .last()
            .and_then(|row| row.get("id"))
            .and_then(value_id)
            .map(str::to_owned);
        if last_id.is_none() {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        rows.extend(page);
    }
    Err(AccountHostValidationError::RiskEvidence)
}

fn account_position_notionals(
    catalogue: &str,
    payload: &str,
    generation: u64,
) -> Result<Vec<Decimal>, AccountHostValidationError> {
    let rows: Vec<Value> =
        serde_json::from_str(payload).map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let mut values = Vec::new();
    for row in rows {
        let item = row
            .as_object()
            .ok_or(AccountHostValidationError::RiskEvidence)?;
        let contracts = decimal_value(item.get("size"))?.abs();
        if contracts.is_zero() {
            continue;
        }
        let rules = contract_rules(
            catalogue,
            symbol_for_contract(item.get("contract"))?,
            generation,
        )?;
        let mark = decimal_value(item.get("mark_price"))?;
        if mark <= Decimal::ZERO {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        let quantity = contracts
            .checked_mul(rules.quanto_multiplier)
            .ok_or(AccountHostValidationError::Notional)?;
        values.push(
            quantity
                .checked_mul(mark)
                .ok_or(AccountHostValidationError::Notional)?,
        );
    }
    Ok(values)
}

fn account_entry_order_notionals(
    catalogue: &str,
    rows: &[Value],
    generation: u64,
) -> Result<Vec<Decimal>, AccountHostValidationError> {
    let mut values = Vec::new();
    for row in rows {
        let item = row
            .as_object()
            .ok_or(AccountHostValidationError::RiskEvidence)?;
        let rules = contract_rules(
            catalogue,
            symbol_for_contract(item.get("contract"))?,
            generation,
        )?;
        let reduce_only = item
            .get("is_reduce_only")
            .and_then(Value::as_bool)
            .ok_or(AccountHostValidationError::RiskEvidence)?;
        let size = decimal_value(item.get("size"))?.abs();
        let left = decimal_value(item.get("left"))?.abs();
        let price = decimal_value(item.get("price"))?;
        if left > size || left <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        if !reduce_only {
            let quantity = left
                .checked_mul(rules.quanto_multiplier)
                .ok_or(AccountHostValidationError::Notional)?;
            values.push(
                quantity
                    .checked_mul(price)
                    .ok_or(AccountHostValidationError::Notional)?,
            );
        }
    }
    Ok(values)
}

fn contract_rules(
    catalogue: &str,
    symbol: Symbol,
    generation: u64,
) -> Result<GateContractRules, AccountHostValidationError> {
    let contracts: Vec<Value> =
        serde_json::from_str(catalogue).map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let native = format!("{}_USDT", symbol.base());
    let mut found = contracts
        .into_iter()
        .filter(|value| value.get("name").and_then(Value::as_str) == Some(native.as_str()));
    let value = found
        .next()
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    if found.next().is_some() {
        return Err(AccountHostValidationError::RiskEvidence);
    }
    parse_contract_rules(&value, symbol, generation)
        .map_err(|_| AccountHostValidationError::RiskEvidence)
}

fn symbol_for_contract(value: Option<&Value>) -> Result<Symbol, AccountHostValidationError> {
    let native = value
        .and_then(Value::as_str)
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    let base = native
        .strip_suffix("_USDT")
        .filter(|base| !base.is_empty())
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    format!("{base}/USDT")
        .parse()
        .map_err(|_| AccountHostValidationError::RiskEvidence)
}

fn decimal_value(value: Option<&Value>) -> Result<Decimal, AccountHostValidationError> {
    match value {
        Some(Value::String(raw)) => raw
            .parse()
            .map_err(|_| AccountHostValidationError::RiskEvidence),
        Some(Value::Number(raw)) => raw
            .to_string()
            .parse()
            .map_err(|_| AccountHostValidationError::RiskEvidence),
        _ => Err(AccountHostValidationError::RiskEvidence),
    }
}

fn page_cursor(payload: &str) -> Result<(Option<String>, bool), GateAccountGatewayError> {
    let rows: Vec<Value> =
        serde_json::from_str(payload).map_err(|_| GateAccountGatewayError::Readback)?;
    if rows.len() > GATE_PRIVATE_PAGE_LIMIT {
        return Err(GateAccountGatewayError::Readback);
    }
    let last = rows
        .last()
        .and_then(|row| row.get("id"))
        .and_then(value_id)
        .map(str::to_owned);
    if rows.len() == GATE_PRIVATE_PAGE_LIMIT && last.is_none() {
        return Err(GateAccountGatewayError::Readback);
    }
    Ok((last, rows.len() < GATE_PRIVATE_PAGE_LIMIT))
}

fn value_id(value: &Value) -> Option<&str> {
    match value {
        Value::String(value)
            if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            Some(value)
        }
        _ => None,
    }
}

fn now_ms() -> Result<u64, GateAccountGatewayError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GateAccountGatewayError::Clock)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| GateAccountGatewayError::Clock)
}

fn rejected(reason: &str) -> AccountGatewayResult {
    AccountGatewayResult::Rejected {
        reason: reason.to_owned(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GateAccountGatewayError {
    #[error("Gate account binding is invalid")]
    Binding,
    #[error("Gate credentials are unavailable")]
    Credentials,
    #[error("Gate account gateway runtime could not start")]
    Runtime,
    #[error("Gate account gateway clock is invalid")]
    Clock,
    #[error("Gate account gateway transport failed: {0}")]
    Transport(GateTransportError),
    #[error("Gate contract rules are invalid")]
    Rules,
    #[error("Gate selected contract rules changed during this resident generation")]
    RulesChanged,
    #[error("Gate signed private readback is incomplete")]
    Readback,
    #[error("Gate attempt identity exhausted")]
    Attempt,
}

#[cfg(test)]
#[path = "account_gateway_tests.rs"]
mod tests;
