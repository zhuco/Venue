use std::{
    collections::{BTreeMap, BTreeSet},
    str,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use serde_json::Value;
use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{
    Asset, ExecutionCommand, FieldState, Fill, LimitTimeInForce, MarketDelta, MarketSnapshot,
    NativeOrderFamily, OrderCommand, OrderSide, OrderState, PositionSide, Price, PublicBar,
    PublicTicker, PublicTrade, Symbol,
};
use venue_execution::{
    AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
    AccountInstrumentIdentity, AccountLimitNormalizationIntent, AccountPhysicalGateway,
    AccountPricedLimitIntent, AccountQuoteToUsdtRate, AccountRecoveryOutcome,
    AccountRecoveryReport, AccountRecoveryRequest, AccountRiskAmount, AccountRiskEvidence,
    SignedAccountBalance, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact, SignedUnknownResult,
    command_matches_readback_order,
};
use venue_gateway_api::{GatewayBinding, PublicMarketBinding};

use crate::private::{RecentFillsCursor, USER_TRADES_PAGE_LIMIT};
#[path = "account_gateway_limit.rs"]
mod account_gateway_limit;
use account_gateway_limit::{
    normalize_fresh_limit, readback_policy_matches_command, snapshot_created_at_ms,
    snapshot_regular_order_quantities,
};
#[path = "account_gateway_private_stream.rs"]
mod account_gateway_private_stream;
pub use account_gateway_private_stream::{BinancePrivateAccountEvent, BinancePrivateFillEvent};
#[path = "account_gateway_projection.rs"]
mod account_gateway_projection;
#[cfg(test)]
use account_gateway_private_stream::{
    PRIVATE_STREAM_MAX_RECONNECT_DELAY, normalize_private_stream_event,
    private_stream_reconnect_delay,
};
use account_gateway_private_stream::{
    PrivateStreamReconnectState, normalize_private_stream_event_for_symbols,
};
#[path = "account_gateway_symbol_dispatch.rs"]
mod account_gateway_symbol_dispatch;
use crate::{
    BinanceAccountBinding, BinanceConfig, BinanceCredentials, BinanceHttpTransport,
    BinanceInstrumentRules, BinancePhysicalMutationOutcome, BinancePrivateReadScope,
    BinancePrivateReadbackCandidate, BinancePrivateWsTransport, BinancePublicKline,
    BinancePublicWsTransport, BinanceRawPublicFrame, BinanceTransportError, BinanceTransportLimits,
    build_account_config_request, build_account_request, build_account_wide_algo_orders_request,
    build_account_wide_positions_request, build_account_wide_regular_orders_request,
    build_algo_orders_request, build_exact_order_for_native_symbol_request,
    build_exact_order_request, build_fills_for_native_symbol_request, build_fills_request,
    build_position_mode_request, build_positions_request, build_regular_orders_request,
    complete_private_readback, connect_private_ws, connect_public_ws, parse_instrument_rules,
    parse_native_instrument_rules, parse_public_market_agg_trade, parse_public_market_bbo,
    parse_public_market_depth_delta, parse_public_market_kline,
    parse_public_market_rest_depth_snapshot, prepare_execution_command, settle_mutation_ack,
};

/// Binance Portfolio Margin implementation of the one-account host boundary. Raw transport
/// remains private to this adapter; only a host-issued, linear permit can reach a mutation.
pub struct BinanceAccountGateway {
    runtime: Runtime,
    config: BinanceConfig,
    credentials: BinanceCredentials,
    transport: BinanceHttpTransport,
    rules: BinanceInstrumentRules,
    rules_by_symbol: BTreeMap<Symbol, BinanceInstrumentRules>,
    private: BinancePrivateReadbackCandidate,
    /// Stable for this gateway process. A reconnect starts a new Account gateway instead of
    /// treating a REST collection or a websocket frame as a new connection.
    connection_generation: u64,
    private_generation: u64,
    next_attempt_id: u64,
    private_stream: Option<BinancePrivateWsTransport>,
    private_stream_keepalive_at: Option<Instant>,
    private_stream_reconnect: PrivateStreamReconnectState,
    public_stream: Option<BinancePublicWsTransport>,
    public_stream_failed: bool,
    public_snapshot_pending: bool,
    rolling_dispatch_cache: Option<account_gateway_symbol_dispatch::BinanceRollingDispatchCache>,
}

const PRIVATE_STREAM_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30 * 60);
const SIGNED_SNAPSHOT_COLLECTION_ATTEMPTS: u8 = 3;
const SIGNED_SNAPSHOT_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);

/// One normalized, read-only public fact from the fixed Binance combined stream.  It is not a
/// strategy decision and has no relation to private account generations or mutation authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinancePublicMarketEvent {
    DepthSnapshot(MarketSnapshot),
    Bbo(PublicTicker),
    Depth(MarketDelta),
    Trade(PublicTrade),
    ClosedBar(PublicBar),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceGridBootstrapMarketFacts {
    pub rules: BinanceInstrumentRules,
    pub bid: Price,
    pub ask: Price,
    pub observed_at_ms: u64,
}

impl BinanceAccountGateway {
    /// Performs a public rule read and a complete signed private read. Construction is read-only.
    pub fn connect_from_environment(
        binding: GatewayBinding,
        limits: BinanceTransportLimits,
    ) -> Result<Self, BinanceAccountGatewayError> {
        Self::connect_from_environment_for_symbols(
            binding.clone(),
            BTreeSet::from([binding.symbol.clone()]),
            limits,
        )
    }

    /// Creates one authenticated account adapter with a finite, canonical rules catalogue.
    /// It intentionally does not create a second gateway, runtime, private generation or writer.
    pub fn connect_from_environment_for_symbols(
        binding: GatewayBinding,
        symbols: BTreeSet<Symbol>,
        limits: BinanceTransportLimits,
    ) -> Result<Self, BinanceAccountGatewayError> {
        let credentials = BinanceCredentials::from_environment()
            .map_err(|_| BinanceAccountGatewayError::Credentials)?;
        Self::connect_with_credentials_for_symbols(binding, symbols, credentials, limits)
    }

    /// Credential-owning hosts such as the KOL executor decrypt from their approved database
    /// boundary. This constructor deliberately has no API-key environment fallback.
    pub fn connect_with_credentials_for_symbols(
        binding: GatewayBinding,
        symbols: BTreeSet<Symbol>,
        credentials: BinanceCredentials,
        limits: BinanceTransportLimits,
    ) -> Result<Self, BinanceAccountGatewayError> {
        let config = BinanceConfig::for_binding(BinanceAccountBinding::PortfolioMarginUm, &binding)
            .map_err(|_| BinanceAccountGatewayError::Binding)?;
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| BinanceAccountGatewayError::Runtime)?;
        let connection_generation = now_ms()?;
        let private_generation = 1;
        let transport = BinanceHttpTransport::new(
            config.clone(),
            connection_generation,
            private_generation,
            limits,
        )
        .map_err(BinanceAccountGatewayError::Transport)?;
        runtime
            .block_on(transport.synchronize_clock())
            .map_err(BinanceAccountGatewayError::Transport)?;
        let rules_by_symbol = runtime.block_on(fetch_rules_catalog(
            &transport,
            &binding,
            &symbols,
            connection_generation,
        ))?;
        let rules = rules_by_symbol
            .get(&binding.symbol)
            .cloned()
            .ok_or(BinanceAccountGatewayError::Instrument)?;
        let private = runtime.block_on(fetch_private(
            &transport,
            &credentials,
            &config,
            &rules,
            private_generation,
            1,
        ))?;
        Ok(Self {
            runtime,
            config,
            credentials,
            transport,
            rules,
            rules_by_symbol,
            private,
            connection_generation,
            private_generation,
            next_attempt_id: 2,
            private_stream: None,
            private_stream_keepalive_at: None,
            private_stream_reconnect: PrivateStreamReconnectState::default(),
            public_stream: None,
            public_stream_failed: false,
            public_snapshot_pending: true,
            rolling_dispatch_cache: None,
        })
    }

    fn take_attempt_id(&mut self) -> Result<u64, BinanceAccountGatewayError> {
        let attempt = self.next_attempt_id;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(BinanceAccountGatewayError::Attempt)?;
        Ok(attempt)
    }

    fn refresh_private(&mut self) -> Result<(), BinanceAccountGatewayError> {
        self.rolling_dispatch_cache = None;
        let current = self.runtime.block_on(fetch_rules_catalog(
            &self.transport,
            self.config.gateway_binding(),
            &self.rules_by_symbol.keys().cloned().collect(),
            self.rules.instrument.generation,
        ))?;
        // A changed instrument rule requires a new transport generation and a new recovery turn;
        // using the previous generation to send is deliberately rejected rather than guessed.
        if current != self.rules_by_symbol {
            return Err(BinanceAccountGatewayError::RulesChanged);
        }
        let attempt = self.take_attempt_id()?;
        let next_private_generation = self.next_private_generation()?;
        let transport = self.transport_for_private_generation(next_private_generation)?;
        let private = self.runtime.block_on(fetch_private(
            &transport,
            &self.credentials,
            &self.config,
            &self.rules,
            next_private_generation,
            attempt,
        ))?;
        self.transport = transport;
        self.private = private;
        self.rules_by_symbol = current;
        self.private_generation = next_private_generation;
        // The authenticated account stream has its own immutable socket generation. A complete
        // REST refresh advances account authority but does not invalidate that socket or discard
        // frames accumulated while mutations were being confirmed.
        Ok(())
    }

    fn next_private_generation(&self) -> Result<u64, BinanceAccountGatewayError> {
        next_private_generation(self.private_generation)
    }

    fn transport_for_private_generation(
        &self,
        private_generation: u64,
    ) -> Result<BinanceHttpTransport, BinanceAccountGatewayError> {
        let mut transport = BinanceHttpTransport::new(
            self.config.clone(),
            self.rules.instrument.generation,
            private_generation,
            self.transport.recovery_limits(),
        )
        .map_err(BinanceAccountGatewayError::Transport)?;
        transport
            .inherit_synchronized_clock(&self.transport)
            .map_err(BinanceAccountGatewayError::Transport)?;
        Ok(transport)
    }

    fn ensure_private_stream(&mut self) -> Result<bool, BinanceAccountGatewayError> {
        if self.private_stream.is_none() {
            self.rolling_dispatch_cache = None;
            if self.private_stream_reconnect.waiting(Instant::now()) {
                return Ok(false);
            }
            let listen_key = match self
                .runtime
                .block_on(self.transport.create_listen_key(&self.credentials))
            {
                Ok(listen_key) => listen_key,
                Err(_) => {
                    return if self.record_private_stream_failure() {
                        Err(BinanceAccountGatewayError::PrivateStream)
                    } else {
                        Ok(false)
                    };
                }
            };
            let stream = match self.runtime.block_on(connect_private_ws(
                &self.config,
                self.rules.instrument.generation,
                self.private_generation,
                listen_key,
                self.transport.recovery_limits(),
            )) {
                Ok(stream) => stream,
                Err(_) => {
                    return if self.record_private_stream_failure() {
                        Err(BinanceAccountGatewayError::PrivateStream)
                    } else {
                        Ok(false)
                    };
                }
            };
            self.private_stream = Some(stream);
            self.private_stream_keepalive_at =
                Some(Instant::now() + PRIVATE_STREAM_KEEPALIVE_INTERVAL);
            self.private_stream_reconnect.record_connected();
        }
        Ok(true)
    }

    /// Establishes the authenticated account stream before startup cancellation or placement.
    /// It does not consume a frame and does not obtain mutation authority.
    pub fn prime_private_stream(&mut self) -> Result<(), BinanceAccountGatewayError> {
        if self.ensure_private_stream()? {
            Ok(())
        } else {
            Err(BinanceAccountGatewayError::PrivateStream)
        }
    }

    /// Opens (once) and polls the bounded private execution stream. This method is strictly
    /// read-only. A disconnect drops the stream and returns an error; it never reconnects under
    /// the same generation or fabricates a newer signed snapshot.
    pub fn poll_private_fill(
        &mut self,
    ) -> Result<Option<BinancePrivateAccountEvent>, BinanceAccountGatewayError> {
        if !self.ensure_private_stream()? {
            return Ok(None);
        }
        if self
            .private_stream_keepalive_at
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            let result = match self.private_stream.as_ref() {
                Some(_) => self
                    .runtime
                    .block_on(self.transport.keepalive_listen_key(&self.credentials)),
                None => Err(BinanceTransportError::Protocol),
            };
            if result.is_err() {
                return if self.record_private_stream_failure() {
                    Err(BinanceAccountGatewayError::PrivateStream)
                } else {
                    Ok(None)
                };
            }
            self.private_stream_keepalive_at =
                Some(Instant::now() + PRIVATE_STREAM_KEEPALIVE_INTERVAL);
        }
        let result = match self.private_stream.as_mut() {
            Some(stream) => self.runtime.block_on(stream.poll_raw_frame()),
            None => return Err(BinanceAccountGatewayError::PrivateStream),
        };
        let stream_private_generation = self
            .private_stream
            .as_ref()
            .map(BinancePrivateWsTransport::private_generation)
            .ok_or(BinanceAccountGatewayError::PrivateStream)?;
        match result {
            Ok(Some(frame)) => match normalize_private_stream_event_for_symbols(
                frame,
                self.config.gateway_binding(),
                &self.rules_by_symbol.keys().cloned().collect(),
                self.rules.instrument.generation,
                stream_private_generation,
                self.private_generation,
            ) {
                Ok(event) => {
                    self.private_stream_reconnect.record_valid_frame();
                    Ok(event)
                }
                Err(error) => {
                    if self.record_private_stream_failure() {
                        Err(error)
                    } else {
                        Ok(None)
                    }
                }
            },
            Ok(None) => Ok(None),
            Err(_) => {
                if self.record_private_stream_failure() {
                    Err(BinanceAccountGatewayError::PrivateStream)
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Performs a new complete signed account read after a private-stream loss boundary. The
    /// returned fills are normalized facts only; raw REST pages and listen keys never leave the
    /// adapter. The durable caller deduplicates their native trade IDs against stream fills.
    pub fn reconcile_private_stream_gap(
        &mut self,
    ) -> Result<Vec<BinancePrivateAccountEvent>, BinanceAccountGatewayError> {
        self.refresh_private()?;
        let received_at_ms = now_ms()?;
        Ok(self
            .private
            .fills()
            .iter()
            .cloned()
            .map(|fill| {
                BinancePrivateAccountEvent::Fill(BinancePrivateFillEvent {
                    stream_private_generation: self.private_generation,
                    private_generation: self.private_generation,
                    received_at_ms,
                    client_order_id: FieldState::Missing,
                    fill,
                })
            })
            .collect())
    }

    fn record_private_stream_failure(&mut self) -> bool {
        self.rolling_dispatch_cache = None;
        self.private_stream = None;
        self.private_stream_keepalive_at = None;
        self.private_stream_reconnect.record_failure(
            Instant::now(),
            self.connection_generation,
            self.private_generation,
        )
    }

    /// Opens exactly one bounded public stream and returns at most one normalized event. A
    /// disconnect permanently fences this gateway instance: reopening under the old rules
    /// generation would create an unprovable public sequence gap.
    pub fn poll_public_market(
        &mut self,
    ) -> Result<Option<BinancePublicMarketEvent>, BinanceAccountGatewayError> {
        if self.public_stream_failed {
            return Err(BinanceAccountGatewayError::PublicStream);
        }
        if self.public_stream.is_none() {
            let stream = self.runtime.block_on(connect_public_ws(
                &self.config,
                self.rules.instrument.generation,
                self.transport.recovery_limits(),
            ))?;
            self.public_stream = Some(stream);
        }
        if self.public_snapshot_pending {
            let response = self.runtime.block_on(
                self.transport
                    .fetch_usd_m_depth_snapshot(&self.rules.native_symbol),
            )?;
            let payload = str::from_utf8(&response.payload)
                .map_err(|_| BinanceAccountGatewayError::PublicStream)?;
            let binding =
                PublicMarketBinding::binance_usds_m(self.config.gateway_binding().symbol.clone())
                    .map_err(|_| BinanceAccountGatewayError::PublicStream)?;
            let snapshot = parse_public_market_rest_depth_snapshot(
                payload,
                &binding,
                self.rules.instrument.generation,
            )
            .map_err(|_| BinanceAccountGatewayError::PublicStream)?;
            self.public_snapshot_pending = false;
            return Ok(Some(BinancePublicMarketEvent::DepthSnapshot(snapshot)));
        }
        let result = match self.public_stream.as_mut() {
            Some(stream) => self.runtime.block_on(stream.poll_raw_frame()),
            None => return Err(BinanceAccountGatewayError::PublicStream),
        };
        match result {
            Ok(Some(frame)) => normalize_public_stream_event(
                frame,
                self.config.gateway_binding(),
                self.rules.instrument.generation,
            ),
            Ok(None) => Ok(None),
            Err(_) => {
                self.public_stream = None;
                self.public_stream_failed = true;
                Err(BinanceAccountGatewayError::PublicStream)
            }
        }
    }

    /// One bounded, read-only BBO/rules collection for Grid epoch installation. It never changes
    /// a generation merely because it was queried; changed rules fail closed for this session.
    pub fn fresh_grid_bootstrap_market(
        &mut self,
    ) -> Result<BinanceGridBootstrapMarketFacts, BinanceAccountGatewayError> {
        let rules = self.runtime.block_on(fetch_rules(
            &self.transport,
            self.config.gateway_binding(),
            self.rules.instrument.generation,
        ))?;
        if rules != self.rules {
            return Err(BinanceAccountGatewayError::RulesChanged);
        }
        let response = self
            .runtime
            .block_on(self.transport.fetch_usd_m_book_ticker(&rules.native_symbol))?;
        parse_grid_bootstrap_bbo(
            &response.payload,
            self.config.gateway_binding(),
            &rules,
            now_ms()?,
        )
    }

    /// Reads only a fresh BBO after the opening wave has already revalidated this gateway's
    /// immutable rules. Bootstrap calls the full rules endpoint once after the closing wave, then
    /// uses this bounded public read for each remaining opening child so a large exchange-info
    /// payload is not downloaded before every individual order.
    pub fn fresh_grid_opening_market_after_rules_validation(
        &mut self,
    ) -> Result<BinanceGridBootstrapMarketFacts, BinanceAccountGatewayError> {
        let rules = self.rules.clone();
        let response = self
            .runtime
            .block_on(self.transport.fetch_usd_m_book_ticker(&rules.native_symbol))?;
        parse_grid_bootstrap_bbo(
            &response.payload,
            self.config.gateway_binding(),
            &rules,
            now_ms()?,
        )
    }

    fn dispatch_permit(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        account_gateway_symbol_dispatch::dispatch_catalog_permit(self, permit)
    }

    fn account_risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        let stage = AccountHostValidationError::RiskEvidenceStage;
        let attempt = self.take_attempt_id().map_err(|_| stage("attempt"))?;
        let next_private_generation = self
            .next_private_generation()
            .map_err(|_| stage("private_generation"))?;
        let transport = self
            .transport_for_private_generation(next_private_generation)
            .map_err(|_| stage("transport"))?;
        let evidence = self.runtime.block_on(fetch_account_wide_risk(
            &transport,
            &self.credentials,
            &self.config,
            &self.rules,
            next_private_generation,
            attempt,
        ))?;
        // Risk evidence is a fresh admission candidate, not an installed account snapshot.
        // Installing its generation here would invalidate Runtime's exact signed Grid surface
        // without publishing the matching snapshot or reconnecting the private ingress.
        Ok(evidence)
    }
}

fn next_private_generation(current: u64) -> Result<u64, BinanceAccountGatewayError> {
    current
        .checked_add(1)
        .filter(|generation| *generation > 0)
        .ok_or(BinanceAccountGatewayError::Attempt)
}

fn binding_for_symbol(
    account_binding: &GatewayBinding,
    symbol: Symbol,
) -> Result<GatewayBinding, BinanceAccountGatewayError> {
    GatewayBinding::new(
        account_binding.venue,
        account_binding.mode,
        account_binding.trading_account_id.clone(),
        symbol,
    )
    .map_err(|_| BinanceAccountGatewayError::Binding)
}

impl AccountPhysicalGateway for BinanceAccountGateway {
    type Error = BinanceAccountGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.config.gateway_binding()
    }

    fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        self.current_instrument_for(&self.config.gateway_binding().symbol.clone())
    }

    fn current_instrument_for(
        &mut self,
        symbol: &Symbol,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        // The configured rule generation is an identity, not a TTL.  Refreshing it here is only
        // acceptable when the authoritative exchangeInfo row remains byte-for-byte equivalent;
        // otherwise callers must recover under a new signed rules generation.
        let requested = BTreeSet::from([symbol.clone()]);
        let mut current = self
            .runtime
            .block_on(fetch_rules_catalog(
                &self.transport,
                self.config.gateway_binding(),
                &requested,
                self.rules.instrument.generation,
            ))
            .map_err(|_| AccountHostValidationError::Instrument)?;
        let current = current
            .remove(symbol)
            .ok_or(AccountHostValidationError::Instrument)?;
        if self.rules_by_symbol.get(symbol) != Some(&current) {
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

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        if request.binding() != self.config.gateway_binding() {
            return Err(BinanceAccountGatewayError::Binding);
        }
        if request.configured_symbols().iter().collect::<BTreeSet<_>>()
            != self.rules_by_symbol.keys().collect()
        {
            return Err(BinanceAccountGatewayError::Binding);
        }
        self.refresh_private()?;
        let observed_at_ms = now_ms()?;
        let mut outcomes = Vec::with_capacity(request.unresolved().len());
        for command in request.unresolved() {
            let client_id = match command {
                ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
                _ => command
                    .native_client_id()
                    .ok_or(BinanceAccountGatewayError::Readback)?
                    .as_str(),
            };
            // A missing/malformed/transport-failed exact lookup is not evidence of absence.
            // Preserve UNKNOWN in the WAL and let the next signed reconciliation turn retry the
            // read only; this path never reconstructs or re-dispatches the mutation.
            let outcome = match build_exact_order_request(self.private.scope(), client_id) {
                Ok(exact) => {
                    match self
                        .transport
                        .signing_timestamp_ms()
                        .ok()
                        .and_then(|timestamp| {
                            self.runtime
                                .block_on(self.transport.execute_read(
                                    &self.credentials,
                                    &exact,
                                    timestamp,
                                ))
                                .ok()
                        }) {
                        Some(page) => match str::from_utf8(&page.payload).ok().and_then(|payload| {
                            crate::private::parse_order(payload, &command.mutation_owner().symbol)
                                .ok()
                        }) {
                            Some(order)
                                if matches!(
                                    &order.client_order_id,
                                    FieldState::Known(value) if value == client_id
                                ) && readback_policy_matches_command(command, &order) =>
                            {
                                if order.state == OrderState::Rejected {
                                    AccountRecoveryOutcome::rejected(
                                        command.command_id().clone(),
                                        "binance_rejected".to_owned(),
                                    )
                                } else {
                                    AccountRecoveryOutcome::accepted(
                                        command.command_id().clone(),
                                        order.order_id,
                                    )
                                }
                            }
                            Some(_) | None => {
                                AccountRecoveryOutcome::still_unknown(command.command_id().clone())
                            }
                        },
                        None => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
                    }
                }
                Err(_) => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
            };
            outcomes.push(outcome);
        }
        AccountRecoveryReport::new(
            self.config.gateway_binding().clone(),
            observed_at_ms,
            outcomes,
        )
        .map_err(|_| BinanceAccountGatewayError::Readback)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        self.account_risk_evidence()
    }

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        if request.binding() != self.config.gateway_binding() {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        if request.configured_symbols().iter().collect::<BTreeSet<_>>()
            != self.rules_by_symbol.keys().collect()
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        self.rolling_dispatch_cache = None;
        let (snapshot, transport, next_private_generation) = retry_signed_snapshot_collection(
            || {
                let attempt = self
                    .take_attempt_id()
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
                let next_private_generation = self
                    .next_private_generation()
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
                let transport = self
                    .transport_for_private_generation(next_private_generation)
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
                let snapshot = self.runtime.block_on(fetch_account_wide_snapshot(
                    BinanceSnapshotCollection {
                        transport: &transport,
                        credentials: &self.credentials,
                        config: &self.config,
                        selected_rules: &self.rules,
                        connection_generation: self.connection_generation,
                        private_generation: next_private_generation,
                        rules_generation: self.rules.instrument.generation,
                        attempt_id: attempt,
                        recovery: request,
                    },
                ))?;
                Ok((snapshot, transport, next_private_generation))
            },
            std::thread::sleep,
        )?;
        self.transport = transport;
        self.private_generation = next_private_generation;
        Ok(snapshot)
    }

    fn normalize_limit_intent(
        &mut self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        intent.validate()?;
        let rules = self
            .rules_by_symbol
            .get(&intent.owner.symbol)
            .cloned()
            .ok_or(AccountHostValidationError::Scope)?;
        let current = self
            .runtime
            .block_on(fetch_rules_catalog(
                &self.transport,
                self.config.gateway_binding(),
                &BTreeSet::from([intent.owner.symbol.clone()]),
                rules.instrument.generation,
            ))
            .map_err(|_| AccountHostValidationError::Command)?;
        if current.get(&intent.owner.symbol) != Some(&rules) {
            return Err(AccountHostValidationError::Command);
        }
        let raw = self
            .runtime
            .block_on(self.transport.fetch_usd_m_book_ticker(&rules.native_symbol))
            .map_err(|_| AccountHostValidationError::Command)?;
        normalize_fresh_limit(
            intent,
            &rules,
            &binding_for_symbol(self.config.gateway_binding(), intent.owner.symbol.clone())
                .map_err(|_| AccountHostValidationError::Scope)?,
            &raw.payload,
            now_ms().map_err(|_| AccountHostValidationError::Command)?,
        )
    }

    fn normalize_priced_limit_intent(
        &mut self,
        intent: &AccountPricedLimitIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        account_gateway_limit::normalize_priced_limit_intent(self, intent)
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatch_permit(permit)
    }
}

fn retry_signed_snapshot_collection<T, E>(
    mut collect: impl FnMut() -> Result<T, E>,
    mut wait: impl FnMut(Duration),
) -> Result<T, E> {
    let mut attempt = 1_u8;
    loop {
        match collect() {
            Ok(value) => return Ok(value),
            Err(_) if attempt < SIGNED_SNAPSHOT_COLLECTION_ATTEMPTS => {
                wait(SIGNED_SNAPSHOT_RETRY_BASE_DELAY * u32::from(attempt));
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn fetch_rules(
    transport: &BinanceHttpTransport,
    binding: &GatewayBinding,
    generation: u64,
) -> Result<BinanceInstrumentRules, BinanceAccountGatewayError> {
    let response = transport
        .fetch_usd_m_exchange_info()
        .await
        .map_err(BinanceAccountGatewayError::Transport)?;
    let payload =
        str::from_utf8(&response.payload).map_err(|_| BinanceAccountGatewayError::Instrument)?;
    parse_instrument_rules(payload, binding.symbol.clone(), generation)
        .map_err(|_| BinanceAccountGatewayError::Instrument)
}

async fn fetch_rules_catalog(
    transport: &BinanceHttpTransport,
    binding: &GatewayBinding,
    symbols: &BTreeSet<Symbol>,
    generation: u64,
) -> Result<BTreeMap<Symbol, BinanceInstrumentRules>, BinanceAccountGatewayError> {
    if symbols.is_empty() || !symbols.contains(&binding.symbol) {
        return Err(BinanceAccountGatewayError::Binding);
    }
    let response = transport
        .fetch_usd_m_exchange_info()
        .await
        .map_err(BinanceAccountGatewayError::Transport)?;
    let payload =
        str::from_utf8(&response.payload).map_err(|_| BinanceAccountGatewayError::Instrument)?;
    parse_rules_catalog(payload, binding, symbols, generation)
}

fn parse_rules_catalog(
    payload: &str,
    binding: &GatewayBinding,
    symbols: &BTreeSet<Symbol>,
    generation: u64,
) -> Result<BTreeMap<Symbol, BinanceInstrumentRules>, BinanceAccountGatewayError> {
    if symbols.is_empty() || !symbols.contains(&binding.symbol) {
        return Err(BinanceAccountGatewayError::Binding);
    }
    symbols
        .iter()
        .map(|symbol| {
            parse_instrument_rules(payload, symbol.clone(), generation)
                .map(|rules| (symbol.clone(), rules))
                .map_err(|_| BinanceAccountGatewayError::Instrument)
        })
        .collect()
}

async fn fetch_private(
    transport: &BinanceHttpTransport,
    credentials: &BinanceCredentials,
    config: &BinanceConfig,
    rules: &BinanceInstrumentRules,
    private_generation: u64,
    attempt_id: u64,
) -> Result<BinancePrivateReadbackCandidate, BinanceAccountGatewayError> {
    let requested_at_ms = now_ms()?;
    let initial_fills_cursor = RecentFillsCursor {
        observed_through_ms: requested_at_ms
            .checked_sub(1)
            .ok_or(BinanceAccountGatewayError::Clock)?,
        last_trade_id: None,
        last_event_time_ms: None,
    };
    let scope = BinancePrivateReadScope::new(
        config,
        rules,
        private_generation,
        attempt_id,
        requested_at_ms,
    )
    .map_err(|_| BinanceAccountGatewayError::Readback)?;
    let mut pages = Vec::with_capacity(7);
    for request in [
        build_account_request(&scope),
        build_account_config_request(&scope),
        build_position_mode_request(&scope),
        build_positions_request(&scope),
        build_regular_orders_request(&scope),
        build_algo_orders_request(&scope),
    ] {
        let request = request.map_err(|_| BinanceAccountGatewayError::Readback)?;
        pages.push(
            transport
                .execute_read(
                    credentials,
                    &request,
                    transport
                        .signing_timestamp_ms()
                        .map_err(BinanceAccountGatewayError::Transport)?,
                )
                .await
                .map_err(BinanceAccountGatewayError::Transport)?,
        );
    }
    let fills = build_fills_request(
        &scope,
        1,
        initial_fills_cursor,
        initial_fills_cursor.observed_through_ms,
        requested_at_ms,
    )
    .map_err(|_| BinanceAccountGatewayError::Readback)?;
    pages.push(
        transport
            .execute_read(
                credentials,
                &fills,
                transport
                    .signing_timestamp_ms()
                    .map_err(BinanceAccountGatewayError::Transport)?,
            )
            .await
            .map_err(BinanceAccountGatewayError::Transport)?,
    );
    complete_private_readback(
        config,
        rules,
        &scope,
        initial_fills_cursor,
        requested_at_ms,
        pages,
    )
    .map_err(|_| BinanceAccountGatewayError::Readback)
}

const ACCOUNT_WIDE_OPEN_ORDER_ROW_LIMIT: usize = 1_000;
const MAX_ACCOUNT_RISK_QUOTE_ASSETS: usize = 16;
const MAX_ACCOUNT_RISK_RATE_AGE_MS: u64 = 60_000;

/// PAPI's open position/order collection endpoints are account-wide collection endpoints, not
/// cursor endpoints.  A response at their documented row ceiling is deliberately ambiguous, so
/// bootstrap refuses it instead of treating a possible truncated collection as an empty tail.
struct BinanceSnapshotCollection<'a> {
    transport: &'a BinanceHttpTransport,
    credentials: &'a BinanceCredentials,
    config: &'a BinanceConfig,
    selected_rules: &'a BinanceInstrumentRules,
    connection_generation: u64,
    private_generation: u64,
    rules_generation: u64,
    attempt_id: u64,
    recovery: &'a AccountRecoveryRequest,
}

async fn fetch_account_wide_snapshot(
    request: BinanceSnapshotCollection<'_>,
) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
    let BinanceSnapshotCollection {
        transport,
        credentials,
        config,
        selected_rules,
        connection_generation,
        private_generation,
        rules_generation,
        attempt_id,
        recovery,
    } = request;
    let observed_at_ms = now_ms().map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let scope = BinancePrivateReadScope::new(
        config,
        selected_rules,
        private_generation,
        attempt_id,
        observed_at_ms,
    )
    .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let catalogue = transport
        .fetch_usd_m_exchange_info()
        .await
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let catalogue = str::from_utf8(&catalogue.payload)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let account_config =
        signed_snapshot_page(transport, credentials, build_account_config_request(&scope)).await?;
    let account =
        signed_snapshot_page(transport, credentials, build_account_request(&scope)).await?;
    let position_mode =
        signed_snapshot_page(transport, credentials, build_position_mode_request(&scope)).await?;
    let positions = signed_snapshot_page(
        transport,
        credentials,
        build_account_wide_positions_request(&scope),
    )
    .await?;
    let regular = signed_snapshot_page(
        transport,
        credentials,
        build_account_wide_regular_orders_request(&scope),
    )
    .await?;
    let algo = signed_snapshot_page(
        transport,
        credentials,
        build_account_wide_algo_orders_request(&scope),
    )
    .await?;
    let account_config = str::from_utf8(&account_config.payload)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let balances = snapshot_balances(&account.payload)?;
    let position_mode = str::from_utf8(&position_mode.payload)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let capabilities = crate::portfolio::capabilities(account_config, position_mode)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    if !capabilities.can_trade || !capabilities.hedge_position || capabilities.one_way_position {
        return Err(AccountHostValidationError::SignedSnapshot);
    }

    let position_rows = json_rows_snapshot(&positions.payload)?;
    let regular_rows = json_rows_snapshot(&regular.payload)?;
    let algo_rows = json_rows_snapshot(&algo.payload)?;
    if !account_wide_order_rows_are_complete(&regular_rows, &algo_rows) {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let position_facts = snapshot_position_facts(catalogue, &position_rows, private_generation)?;
    let order_facts =
        snapshot_order_facts(catalogue, &regular_rows, &algo_rows, private_generation)?;
    let previous_fills = parse_snapshot_fills_cursor(recovery.previous_fills_cursor())?;
    let fill_symbols = snapshot_fill_symbols(
        &position_rows,
        &regular_rows,
        &algo_rows,
        recovery,
        &previous_fills,
    )?;
    let (fills_cursor, fill_facts) = snapshot_fills_cursor(BinanceSnapshotFillsRequest {
        transport,
        credentials,
        scope: &scope,
        symbols: &fill_symbols,
        previous: previous_fills,
        observed_at_ms,
        catalogue,
        generation: private_generation,
    })
    .await?;
    let unknown_results =
        snapshot_unknown_results(transport, credentials, &scope, recovery).await?;
    SignedAccountSnapshot::complete_with_fills(
        config.gateway_binding().clone(),
        observed_at_ms,
        connection_generation,
        private_generation,
        rules_generation,
        SignedAccountPositionMode::Hedge,
        order_facts,
        position_facts,
        fill_facts,
        fills_cursor,
        unknown_results,
    )
    .and_then(|snapshot| snapshot.with_balances(balances))
    .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

fn normalize_public_stream_event(
    frame: BinanceRawPublicFrame,
    binding: &GatewayBinding,
    rules_generation: u64,
) -> Result<Option<BinancePublicMarketEvent>, BinanceAccountGatewayError> {
    if frame.binding != *binding
        || frame.instrument_generation != rules_generation
        || frame.received_at_ms == 0
    {
        return Err(BinanceAccountGatewayError::PublicStream);
    }
    let payload =
        str::from_utf8(&frame.payload).map_err(|_| BinanceAccountGatewayError::PublicStream)?;
    let value: Value =
        serde_json::from_str(payload).map_err(|_| BinanceAccountGatewayError::PublicStream)?;
    let event = value
        .get("data")
        .unwrap_or(&value)
        .get("e")
        .and_then(Value::as_str)
        .ok_or(BinanceAccountGatewayError::PublicStream)?;
    let public_binding = PublicMarketBinding::binance_usds_m(binding.symbol.clone())
        .map_err(|_| BinanceAccountGatewayError::PublicStream)?;
    let normalized = match event {
        "bookTicker" => parse_public_market_bbo(
            payload,
            &public_binding,
            rules_generation,
            frame.received_at_ms,
        )
        .map(|value| Some(BinancePublicMarketEvent::Bbo(value.into_fact()))),
        "depthUpdate" => {
            parse_public_market_depth_delta(payload, &public_binding, rules_generation)
                .map(|value| Some(BinancePublicMarketEvent::Depth(value.into_fact())))
        }
        "aggTrade" => parse_public_market_agg_trade(
            payload,
            &public_binding,
            rules_generation,
            frame.received_at_ms,
        )
        .map(|value| Some(BinancePublicMarketEvent::Trade(value.into_fact()))),
        "kline" => match parse_public_market_kline(
            payload,
            &public_binding,
            rules_generation,
            frame.received_at_ms,
        ) {
            Ok(BinancePublicKline::Closed(value)) => {
                Ok(Some(BinancePublicMarketEvent::ClosedBar(value.into_fact())))
            }
            Ok(BinancePublicKline::Forming(_)) => Ok(None),
            Err(_) => Err(crate::BinancePublicError::Payload),
        },
        _ => Err(crate::BinancePublicError::Payload),
    };
    normalized.map_err(|_| BinanceAccountGatewayError::PublicStream)
}

fn parse_grid_bootstrap_bbo(
    payload: &[u8],
    binding: &GatewayBinding,
    rules: &BinanceInstrumentRules,
    now: u64,
) -> Result<BinanceGridBootstrapMarketFacts, BinanceAccountGatewayError> {
    let row: Value =
        serde_json::from_slice(payload).map_err(|_| BinanceAccountGatewayError::Readback)?;
    let observed_at_ms = row
        .get("time")
        .and_then(Value::as_u64)
        .filter(|time| *time > 0)
        .filter(|time| *time <= now && now.saturating_sub(*time) <= 3_000)
        .ok_or(BinanceAccountGatewayError::Readback)?;
    if row.get("symbol").and_then(Value::as_str) != Some(rules.native_symbol.as_str())
        || binding.symbol != rules.instrument.symbol
    {
        return Err(BinanceAccountGatewayError::Readback);
    }
    let price = |field| {
        row.get(field)
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<Decimal>().ok())
            .filter(|value| {
                *value > Decimal::ZERO
                    && *value % rules.instrument.price_tick.value() == Decimal::ZERO
            })
            .and_then(|value| Price::new(value).ok())
            .ok_or(BinanceAccountGatewayError::Readback)
    };
    let bid = price("bidPrice")?;
    let ask = price("askPrice")?;
    if bid >= ask {
        return Err(BinanceAccountGatewayError::Readback);
    }
    Ok(BinanceGridBootstrapMarketFacts {
        rules: rules.clone(),
        bid,
        ask,
        observed_at_ms,
    })
}

fn snapshot_balances(
    payload: &[u8],
) -> Result<Vec<SignedAccountBalance>, AccountHostValidationError> {
    let payload =
        str::from_utf8(payload).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let balance = crate::portfolio::parse_account_balance(payload)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let usd = Asset::new("USD").map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    Ok(vec![SignedAccountBalance {
        // PAPI accountEquity and totalAvailableBalance are portfolio-wide USD values. They must
        // not be labelled as a stablecoin until a same-generation conversion proves that rate.
        asset: usd,
        equity: balance.wallet_balance,
        available_margin: Some(balance.available_balance),
    }])
}

fn account_wide_order_rows_are_complete(
    regular: &[serde_json::Map<String, Value>],
    algo: &[serde_json::Map<String, Value>],
) -> bool {
    regular.len() < ACCOUNT_WIDE_OPEN_ORDER_ROW_LIMIT
        && algo.len() < ACCOUNT_WIDE_OPEN_ORDER_ROW_LIMIT
}

async fn signed_snapshot_page(
    transport: &BinanceHttpTransport,
    credentials: &BinanceCredentials,
    request: Result<crate::BinancePrivateReadRequest, crate::BinanceReadbackError>,
) -> Result<crate::BinanceRawPrivatePage, AccountHostValidationError> {
    let request = request.map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    transport
        .execute_read(
            credentials,
            &request,
            transport
                .signing_timestamp_ms()
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
        )
        .await
        .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

fn json_rows_snapshot(
    payload: &[u8],
) -> Result<Vec<serde_json::Map<String, Value>>, AccountHostValidationError> {
    let value: Value =
        serde_json::from_slice(payload).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    value
        .as_array()
        .ok_or(AccountHostValidationError::SignedSnapshot)?
        .iter()
        .map(|row| {
            row.as_object()
                .cloned()
                .ok_or(AccountHostValidationError::SignedSnapshot)
        })
        .collect()
}

fn snapshot_rules(
    catalogue: &str,
    native: &str,
    generation: u64,
) -> Result<BinanceInstrumentRules, AccountHostValidationError> {
    parse_native_instrument_rules(catalogue, native, generation)
        .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

fn snapshot_position_facts(
    catalogue: &str,
    rows: &[serde_json::Map<String, Value>],
    generation: u64,
) -> Result<Vec<SignedAccountPositionFact>, AccountHostValidationError> {
    let mut seen = BTreeSet::new();
    let mut facts = Vec::with_capacity(rows.len());
    for row in rows {
        let native = snapshot_text(row, "symbol")?;
        let rules = snapshot_rules(catalogue, native, generation)?;
        let position_side = match snapshot_text(row, "positionSide")? {
            "LONG" => PositionSide::Long,
            "SHORT" => PositionSide::Short,
            _ => return Err(AccountHostValidationError::SignedSnapshot),
        };
        if !seen.insert((rules.instrument.symbol.clone(), position_side)) {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let raw_quantity = snapshot_decimal(row, "positionAmt")?;
        let quantity = raw_quantity.abs();
        let entry_price = snapshot_optional_positive_decimal(row, "entryPrice")?;
        let mark_price = snapshot_optional_positive_decimal(row, "markPrice")?;
        facts.push(SignedAccountPositionFact {
            symbol: rules.instrument.symbol,
            position_side,
            quantity,
            entry_price,
            mark_price,
        });
    }
    Ok(facts)
}

fn snapshot_order_facts(
    catalogue: &str,
    regular_rows: &[serde_json::Map<String, Value>],
    algo_rows: &[serde_json::Map<String, Value>],
    generation: u64,
) -> Result<Vec<SignedAccountOrderFact>, AccountHostValidationError> {
    let mut client_ids = BTreeSet::new();
    let mut facts = Vec::with_capacity(regular_rows.len().saturating_add(algo_rows.len()));
    for row in regular_rows {
        let native = snapshot_text(row, "symbol")?;
        let rules = snapshot_rules(catalogue, native, generation)?;
        let (quantity, filled_quantity) = snapshot_regular_order_quantities(row)?;
        let client_order_id = snapshot_text(row, "clientOrderId")?.to_owned();
        if !client_ids.insert(client_order_id.clone()) {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        facts.push(SignedAccountOrderFact {
            client_order_id,
            venue_order_id: Some(snapshot_identifier(row, "orderId")?),
            symbol: rules.instrument.symbol,
            family: NativeOrderFamily::UmOrder,
            side: snapshot_side(row)?,
            position_side: snapshot_position_side(row)?,
            quantity,
            limit_price: snapshot_optional_positive_decimal(row, "price")?,
            time_in_force: snapshot_limit_time_in_force(row, "timeInForce")?,
            created_at_ms: snapshot_created_at_ms(row, "time")?,
            reduce_only: snapshot_bool(row, "reduceOnly")?,
            owner: None,
            external: true,
            state: Some(snapshot_order_state(snapshot_text(row, "status")?)?),
            filled_quantity: Some(filled_quantity),
        });
    }
    for row in algo_rows {
        let native = snapshot_text(row, "symbol")?;
        let rules = snapshot_rules(catalogue, native, generation)?;
        let client_order_id = snapshot_text(row, "clientAlgoId")?.to_owned();
        if !client_ids.insert(client_order_id.clone()) || snapshot_bool(row, "closePosition")? {
            // A close-all Algo has no stable remaining quantity on PAPI.  It remains a signed
            // open-order fact only if a future adapter DTO can represent it; do not invent one.
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let quantity = snapshot_decimal(row, "quantity")?;
        if quantity <= Decimal::ZERO {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        facts.push(SignedAccountOrderFact {
            client_order_id,
            venue_order_id: Some(snapshot_identifier(row, "algoId")?),
            symbol: rules.instrument.symbol,
            family: NativeOrderFamily::UmAlgo,
            side: snapshot_side(row)?,
            position_side: snapshot_position_side(row)?,
            quantity,
            limit_price: snapshot_optional_positive_decimal(row, "triggerPrice")?,
            time_in_force: None,
            created_at_ms: snapshot_created_at_ms(row, "time")?,
            reduce_only: snapshot_bool(row, "reduceOnly")?,
            owner: None,
            external: true,
            state: Some(snapshot_order_state(snapshot_text(row, "algoStatus")?)?),
            filled_quantity: None,
        });
    }
    Ok(facts)
}

fn snapshot_order_state(value: &str) -> Result<OrderState, AccountHostValidationError> {
    match value {
        "NEW" => Ok(OrderState::New),
        "PARTIALLY_FILLED" => Ok(OrderState::PartiallyFilled),
        "FILLED" => Ok(OrderState::Filled),
        "CANCELED" => Ok(OrderState::Cancelled),
        "EXPIRED" => Ok(OrderState::Expired),
        "REJECTED" => Ok(OrderState::Rejected),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn snapshot_fill_symbols(
    positions: &[serde_json::Map<String, Value>],
    regular: &[serde_json::Map<String, Value>],
    algo: &[serde_json::Map<String, Value>],
    recovery: &AccountRecoveryRequest,
    previous: &BinanceSnapshotFillsCursor,
) -> Result<BTreeSet<String>, AccountHostValidationError> {
    let mut symbols = recovery
        .configured_symbols()
        .iter()
        .map(crate::native_symbol)
        .collect::<BTreeSet<_>>();
    symbols.extend(previous.by_native_symbol.keys().cloned());
    for row in positions.iter().chain(regular).chain(algo) {
        symbols.insert(snapshot_text(row, "symbol")?.to_owned());
    }
    for command in recovery.unresolved() {
        symbols.insert(crate::native_symbol(&command.mutation_owner().symbol));
    }
    // PAPI userTrades is symbol-scoped.  The only complete account-owned universe available to
    // this adapter is the durable cursor plus current signed facts and unresolved WAL owners;
    // exchangeInfo lists tradable contracts, not this account's history.
    if symbols.is_empty() {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    Ok(symbols)
}

struct BinanceSnapshotFillsRequest<'a> {
    transport: &'a BinanceHttpTransport,
    credentials: &'a BinanceCredentials,
    scope: &'a BinancePrivateReadScope,
    symbols: &'a BTreeSet<String>,
    previous: BinanceSnapshotFillsCursor,
    observed_at_ms: u64,
    catalogue: &'a str,
    generation: u64,
}

async fn snapshot_fills_cursor(
    request: BinanceSnapshotFillsRequest<'_>,
) -> Result<(String, Vec<Fill>), AccountHostValidationError> {
    let BinanceSnapshotFillsRequest {
        transport,
        credentials,
        scope,
        symbols,
        previous,
        observed_at_ms,
        catalogue,
        generation,
    } = request;
    let default_start = observed_at_ms
        .checked_sub(1)
        .filter(|value| *value > 0)
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    let mut fills = Vec::new();
    let mut fill_ids = BTreeSet::new();
    let mut next = previous;
    for native in symbols {
        let mut cursor = next
            .by_native_symbol
            .get(native)
            .copied()
            .unwrap_or(RecentFillsCursor {
                observed_through_ms: default_start,
                last_trade_id: None,
                last_event_time_ms: None,
            });
        let start = cursor.observed_through_ms;
        let mut terminal = false;
        for page_index in 1..=crate::BINANCE_PRIVATE_MAX_PAGES {
            let page_index = u32::try_from(page_index)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let request = build_fills_for_native_symbol_request(
                scope,
                native,
                page_index,
                cursor,
                start,
                observed_at_ms,
            )
            .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let page = transport
                .execute_read(
                    credentials,
                    &request,
                    transport
                        .signing_timestamp_ms()
                        .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                )
                .await
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            let rows = json_rows_snapshot(&page.payload)?;
            advance_snapshot_fill_cursor(&mut cursor, &rows, start)?;
            let rules = snapshot_rules(catalogue, native, generation)?;
            let payload = str::from_utf8(&page.payload)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
            for fill in crate::private::parse_fills(payload, &rules.instrument.symbol)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?
            {
                if !fill_ids.insert((fill.symbol.clone(), fill.fill_id.clone())) {
                    return Err(AccountHostValidationError::SignedSnapshot);
                }
                fills.push(fill);
            }
            if rows.len() < usize::from(USER_TRADES_PAGE_LIMIT) {
                terminal = true;
                break;
            }
        }
        if !terminal {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        cursor.observed_through_ms = cursor.observed_through_ms.max(observed_at_ms);
        next.by_native_symbol.insert(native.clone(), cursor);
    }
    Ok((next.encode(), fills))
}

fn advance_snapshot_fill_cursor(
    cursor: &mut RecentFillsCursor,
    rows: &[serde_json::Map<String, Value>],
    start: u64,
) -> Result<(), AccountHostValidationError> {
    if rows.len() > usize::from(USER_TRADES_PAGE_LIMIT) {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    for row in rows {
        let id = snapshot_u64(row, "id")?;
        let event_time = snapshot_u64(row, "time")?;
        if cursor.last_trade_id.is_some_and(|previous| id <= previous)
            || cursor
                .last_event_time_ms
                .is_some_and(|previous| event_time < previous)
            || event_time < start
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        cursor.last_trade_id = Some(id);
        cursor.last_event_time_ms = Some(event_time);
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct BinanceSnapshotFillsCursor {
    by_native_symbol: std::collections::BTreeMap<String, RecentFillsCursor>,
}

impl BinanceSnapshotFillsCursor {
    fn encode(&self) -> String {
        let mut values = Vec::with_capacity(self.by_native_symbol.len());
        for (symbol, cursor) in &self.by_native_symbol {
            values.push(format!(
                "{symbol},{},{},{}",
                cursor.observed_through_ms,
                cursor
                    .last_trade_id
                    .map_or_else(String::new, |value| value.to_string()),
                cursor
                    .last_event_time_ms
                    .map_or_else(String::new, |value| value.to_string()),
            ));
        }
        format!("binance-fills-v1|{}", values.join(";"))
    }
}

fn parse_snapshot_fills_cursor(
    value: Option<&str>,
) -> Result<BinanceSnapshotFillsCursor, AccountHostValidationError> {
    let Some(value) = value else {
        return Ok(BinanceSnapshotFillsCursor::default());
    };
    let body = value
        .strip_prefix("binance-fills-v1|")
        // Legacy SHA digests have no replay watermark. Reusing one would silently omit fills.
        .ok_or(AccountHostValidationError::SignedSnapshot)?;
    let mut by_native_symbol = std::collections::BTreeMap::new();
    if body.is_empty() {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    for entry in body.split(';') {
        let mut fields = entry.split(',');
        let symbol = fields
            .next()
            .filter(|value| !value.is_empty())
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        let observed_through_ms = fields
            .next()
            .and_then(|value| value.parse().ok())
            .filter(|value: &u64| *value > 0)
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        let last_trade_id = match fields.next() {
            Some("") => None,
            Some(value) => Some(
                value
                    .parse()
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            ),
            None => return Err(AccountHostValidationError::SignedSnapshot),
        };
        let last_event_time_ms = match fields.next() {
            Some("") => None,
            Some(value) => Some(
                value
                    .parse()
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            ),
            None => return Err(AccountHostValidationError::SignedSnapshot),
        };
        if fields.next().is_some()
            || last_trade_id.is_some() != last_event_time_ms.is_some()
            || !symbol
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            || by_native_symbol
                .insert(
                    symbol.to_owned(),
                    RecentFillsCursor {
                        observed_through_ms,
                        last_trade_id,
                        last_event_time_ms,
                    },
                )
                .is_some()
        {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
    }
    Ok(BinanceSnapshotFillsCursor { by_native_symbol })
}

async fn snapshot_unknown_results(
    transport: &BinanceHttpTransport,
    credentials: &BinanceCredentials,
    scope: &BinancePrivateReadScope,
    recovery: &AccountRecoveryRequest,
) -> Result<Vec<SignedUnknownFact>, AccountHostValidationError> {
    let mut values = Vec::with_capacity(recovery.unresolved().len());
    for command in recovery.unresolved() {
        let client_id = match command {
            ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
            _ => command
                .native_client_id()
                .ok_or(AccountHostValidationError::SignedSnapshot)?
                .as_str(),
        };
        let result = match command.native_order_family() {
            Some(NativeOrderFamily::UmOrder) | None => {
                let native = crate::native_symbol(&command.mutation_owner().symbol);
                let exact = build_exact_order_for_native_symbol_request(scope, &native, client_id)
                    .map_err(|_| AccountHostValidationError::SignedSnapshot)?;
                match transport
                    .execute_read(
                        credentials,
                        &exact,
                        transport
                            .signing_timestamp_ms()
                            .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
                    )
                    .await
                {
                    Ok(page) => snapshot_exact_regular_result(&page.payload, command, client_id),
                    Err(_) => SignedUnknownResult::Unknown,
                }
            }
            Some(NativeOrderFamily::UmConditional | NativeOrderFamily::UmAlgo) => {
                // PAPI does not offer a common exact endpoint for these separate namespaces.
                // Their complete current collections prove only that an order is presently open;
                // terminal absence remains Unknown and can never trigger a retry.
                SignedUnknownResult::Unknown
            }
        };
        values.push(SignedUnknownFact {
            command_id: command.command_id().clone(),
            result,
        });
    }
    Ok(values)
}

fn snapshot_exact_regular_result(
    payload: &[u8],
    command: &ExecutionCommand,
    client_id: &str,
) -> SignedUnknownResult {
    let order = match str::from_utf8(payload)
        .ok()
        .and_then(|raw| crate::private::parse_order(raw, &command.mutation_owner().symbol).ok())
    {
        Some(order) => order,
        None => return SignedUnknownResult::Unknown,
    };
    if !command_matches_readback_order(command, &order)
        || !matches!(&order.client_order_id, FieldState::Known(value) if value == client_id)
    {
        return SignedUnknownResult::Unknown;
    }
    match order.state {
        OrderState::Rejected => SignedUnknownResult::Rejected {
            reason: "binance_rejected".to_owned(),
        },
        OrderState::New
        | OrderState::PartiallyFilled
        | OrderState::Filled
        | OrderState::Cancelled
        | OrderState::Expired => SignedUnknownResult::Accepted {
            venue_order_id: order.order_id,
        },
        OrderState::Unknown => SignedUnknownResult::Unknown,
    }
}

fn snapshot_text<'a>(
    row: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, AccountHostValidationError> {
    row.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or(AccountHostValidationError::SignedSnapshot)
}

fn snapshot_identifier(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<String, AccountHostValidationError> {
    match row.get(field) {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(value.clone()),
        Some(Value::Number(value)) if value.as_u64().is_some() => Ok(value.to_string()),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn snapshot_decimal(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Decimal, AccountHostValidationError> {
    snapshot_text(row, field)?
        .parse()
        .map_err(|_| AccountHostValidationError::SignedSnapshot)
}

fn snapshot_optional_positive_decimal(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<Decimal>, AccountHostValidationError> {
    let value = snapshot_decimal(row, field)?;
    if value.is_zero() {
        Ok(None)
    } else if value.is_sign_positive() {
        Ok(Some(value))
    } else {
        Err(AccountHostValidationError::SignedSnapshot)
    }
}

fn snapshot_bool(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<bool, AccountHostValidationError> {
    row.get(field)
        .and_then(Value::as_bool)
        .ok_or(AccountHostValidationError::SignedSnapshot)
}

fn snapshot_limit_time_in_force(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<LimitTimeInForce>, AccountHostValidationError> {
    match row.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => match value.as_str() {
            "GTX" => Ok(Some(LimitTimeInForce::PostOnly)),
            "GTC" => Ok(Some(LimitTimeInForce::Gtc)),
            _ => Ok(None),
        },
        Some(_) => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn snapshot_side(
    row: &serde_json::Map<String, Value>,
) -> Result<OrderSide, AccountHostValidationError> {
    match snapshot_text(row, "side")? {
        "BUY" => Ok(OrderSide::Buy),
        "SELL" => Ok(OrderSide::Sell),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn snapshot_position_side(
    row: &serde_json::Map<String, Value>,
) -> Result<PositionSide, AccountHostValidationError> {
    match snapshot_text(row, "positionSide")? {
        "LONG" => Ok(PositionSide::Long),
        "SHORT" => Ok(PositionSide::Short),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

fn snapshot_u64(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<u64, AccountHostValidationError> {
    match row.get(field) {
        Some(Value::String(value)) => value
            .parse()
            .map_err(|_| AccountHostValidationError::SignedSnapshot),
        Some(Value::Number(value)) => value
            .as_u64()
            .ok_or(AccountHostValidationError::SignedSnapshot),
        _ => Err(AccountHostValidationError::SignedSnapshot),
    }
}

async fn fetch_account_wide_risk(
    transport: &BinanceHttpTransport,
    credentials: &BinanceCredentials,
    config: &BinanceConfig,
    selected_rules: &BinanceInstrumentRules,
    private_generation: u64,
    attempt_id: u64,
) -> Result<AccountRiskEvidence, AccountHostValidationError> {
    let stage = AccountHostValidationError::RiskEvidenceStage;
    let observed_at_ms = now_ms().map_err(|_| stage("clock_start"))?;
    let scope = BinancePrivateReadScope::new(
        config,
        selected_rules,
        private_generation,
        attempt_id,
        observed_at_ms,
    )
    .map_err(|_| stage("scope"))?;
    let catalogue = transport
        .fetch_usd_m_exchange_info()
        .await
        .map_err(|_| stage("exchange_info_read"))?;
    let catalogue = str::from_utf8(&catalogue.payload).map_err(|_| stage("exchange_info_utf8"))?;
    let account_config = signed_page(transport, credentials, build_account_config_request(&scope))
        .await
        .map_err(|_| stage("account_config_read"))?;
    let position_mode = signed_page(transport, credentials, build_position_mode_request(&scope))
        .await
        .map_err(|_| stage("position_mode_read"))?;
    let positions = signed_page(
        transport,
        credentials,
        build_account_wide_positions_request(&scope),
    )
    .await
    .map_err(|_| stage("positions_read"))?;
    let regular = signed_page(
        transport,
        credentials,
        build_account_wide_regular_orders_request(&scope),
    )
    .await
    .map_err(|_| stage("regular_orders_read"))?;
    let algo = signed_page(
        transport,
        credentials,
        build_account_wide_algo_orders_request(&scope),
    )
    .await
    .map_err(|_| stage("algo_orders_read"))?;
    let account_config =
        str::from_utf8(&account_config.payload).map_err(|_| stage("account_config_utf8"))?;
    let position_mode =
        str::from_utf8(&position_mode.payload).map_err(|_| stage("position_mode_utf8"))?;
    let capabilities = crate::portfolio::capabilities(account_config, position_mode)
        .map_err(|_| stage("capabilities_parse"))?;
    if !capabilities.can_trade || !capabilities.hedge_position {
        return Err(stage("capabilities_value"));
    }
    let positions = account_position_notionals(catalogue, &positions.payload, private_generation)
        .map_err(|_| stage("positions_normalize"))?;
    let orders = account_entry_order_notionals(
        catalogue,
        &regular.payload,
        &algo.payload,
        private_generation,
    )
    .map_err(|_| stage("orders_normalize"))?;
    let mut quote_assets = positions
        .iter()
        .chain(orders.iter())
        .map(|amount| amount.asset.clone())
        .collect::<BTreeSet<_>>();
    // The selected binding is the only candidate this gateway can normalize. Include its quote
    // even when the signed account is flat, otherwise a valid SOL/USDC entry could not be
    // valued while preserving the complete all-symbol account totals above.
    quote_assets.insert(
        Asset::new(config.gateway_binding().symbol.quote()).map_err(|_| stage("selected_quote"))?,
    );
    let rates = fetch_quote_to_usdt_rates(transport, &quote_assets, private_generation)
        .await
        .map_err(|_| stage("quote_rates"))?;
    AccountRiskEvidence::complete_with_usdt_valuation(
        config.gateway_binding().clone(),
        now_ms().map_err(|_| stage("clock_finish"))?,
        private_generation,
        positions,
        orders,
        rates,
    )
    .map_err(|_| stage("evidence_complete"))?
    .with_earliest_observation(observed_at_ms)
    .map_err(|_| stage("evidence_window"))
}

/// Public asset-index evidence converts every native quote asset from the complete signed
/// position/order collection. USDT is the sole identity; every other quote uses its own USD
/// index divided by the independently observed USDT USD index.
async fn fetch_quote_to_usdt_rates(
    transport: &BinanceHttpTransport,
    quote_assets: &BTreeSet<Asset>,
    private_generation: u64,
) -> Result<Vec<AccountQuoteToUsdtRate>, AccountHostValidationError> {
    let usdt = Asset::new("USDT").map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let non_usdt = quote_assets
        .iter()
        .filter(|asset| *asset != &usdt)
        .cloned()
        .collect::<Vec<_>>();
    if non_usdt.is_empty() {
        return Ok(Vec::new());
    }
    if non_usdt.len() >= MAX_ACCOUNT_RISK_QUOTE_ASSETS {
        return Err(AccountHostValidationError::RiskEvidence);
    }

    let mut required = non_usdt.clone();
    required.push(usdt.clone());
    let mut usd_per_asset = BTreeMap::new();
    for asset in required {
        let response = transport
            .fetch_usd_m_asset_index(&asset)
            .await
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let payload = str::from_utf8(&response.payload)
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        let evidence = crate::portfolio::parse_usd_conversion_evidence(
            payload,
            asset.clone(),
            private_generation,
            response.received_at_ms,
            MAX_ACCOUNT_RISK_RATE_AGE_MS,
        )
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        if usd_per_asset.insert(asset, evidence).is_some() {
            return Err(AccountHostValidationError::RiskEvidence);
        }
    }
    quote_to_usdt_rates(quote_assets, &usd_per_asset, private_generation)
}

fn quote_to_usdt_rates(
    quote_assets: &BTreeSet<Asset>,
    usd_per_asset: &BTreeMap<Asset, crate::portfolio::UsdConversionEvidence>,
    private_generation: u64,
) -> Result<Vec<AccountQuoteToUsdtRate>, AccountHostValidationError> {
    let usdt = Asset::new("USDT").map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let non_usdt = quote_assets
        .iter()
        .filter(|asset| *asset != &usdt)
        .cloned()
        .collect::<Vec<_>>();
    if non_usdt.is_empty() {
        return Ok(Vec::new());
    }
    if non_usdt.len() >= MAX_ACCOUNT_RISK_QUOTE_ASSETS {
        return Err(AccountHostValidationError::RiskEvidence);
    }
    let usdt_usd = usd_per_asset
        .get(&usdt)
        .filter(|evidence| {
            evidence.private_generation == private_generation
                && evidence.observed_at_ms > 0
                && evidence.usd_per_asset > Decimal::ZERO
        })
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    non_usdt
        .into_iter()
        .map(|asset| {
            let quote_usd = usd_per_asset
                .get(&asset)
                .filter(|evidence| {
                    evidence.private_generation == private_generation
                        && evidence.observed_at_ms > 0
                        && evidence.usd_per_asset > Decimal::ZERO
                })
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            let usdt_per_asset = quote_usd
                .usd_per_asset
                .checked_div(usdt_usd.usd_per_asset)
                .filter(|rate| *rate > Decimal::ZERO)
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            Ok(AccountQuoteToUsdtRate {
                asset,
                usdt_per_asset,
                observed_at_ms: quote_usd.source_time_ms.min(usdt_usd.source_time_ms),
                private_generation,
            })
        })
        .collect()
}

async fn signed_page(
    transport: &BinanceHttpTransport,
    credentials: &BinanceCredentials,
    request: Result<crate::BinancePrivateReadRequest, crate::BinanceReadbackError>,
) -> Result<crate::BinanceRawPrivatePage, AccountHostValidationError> {
    let request = request.map_err(|_| AccountHostValidationError::RiskEvidence)?;
    transport
        .execute_read(
            credentials,
            &request,
            transport
                .signing_timestamp_ms()
                .map_err(|_| AccountHostValidationError::RiskEvidence)?,
        )
        .await
        .map_err(|_| AccountHostValidationError::RiskEvidence)
}

fn account_position_notionals(
    catalogue: &str,
    payload: &[u8],
    generation: u64,
) -> Result<Vec<AccountRiskAmount>, AccountHostValidationError> {
    let rows = json_rows(payload)?;
    let mut notionals = Vec::new();
    for row in rows {
        let quantity = decimal_field(&row, "positionAmt")?;
        if quantity.is_zero() {
            continue;
        }
        let rules = account_rules(catalogue, text_field(&row, "symbol")?, generation)?;
        let mark = decimal_field(&row, "markPrice")?;
        let reported = decimal_field(&row, "notional")?.abs();
        let computed = quantity
            .abs()
            .checked_mul(mark)
            .ok_or(AccountHostValidationError::Notional)?;
        if mark <= Decimal::ZERO || reported != computed.round_dp(8) {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        validate_quantity(&rules, quantity.abs())?;
        notionals.push(AccountRiskAmount {
            asset: quote_asset(&rules)?,
            value: reported,
        });
    }
    Ok(notionals)
}

fn account_entry_order_notionals(
    catalogue: &str,
    regular: &[u8],
    algo: &[u8],
    generation: u64,
) -> Result<Vec<AccountRiskAmount>, AccountHostValidationError> {
    let mut notionals = Vec::new();
    for row in json_rows(regular)? {
        let reduce_only = bool_field(&row, "reduceOnly")?;
        let rules = account_rules(catalogue, text_field(&row, "symbol")?, generation)?;
        let quantity = decimal_field(&row, "origQty")?;
        let filled = decimal_field(&row, "executedQty")?;
        let remaining = quantity
            .checked_sub(filled)
            .ok_or(AccountHostValidationError::RiskEvidence)?;
        let price = decimal_field(&row, "price")?;
        if remaining <= Decimal::ZERO || price <= Decimal::ZERO {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        validate_quantity(&rules, remaining)?;
        if price % rules.instrument.price_tick.value() != Decimal::ZERO {
            return Err(AccountHostValidationError::RiskEvidence);
        }
        if !reduce_only {
            notionals.push(AccountRiskAmount {
                asset: quote_asset(&rules)?,
                value: remaining
                    .checked_mul(price)
                    .ok_or(AccountHostValidationError::Notional)?,
            });
        }
    }
    for row in json_rows(algo)? {
        // Conditional family has several wire shapes. A non-reduce strategy that is not fully
        // normalized must reserve no guessed value: it closes entry admission until reconciled.
        if !bool_field(&row, "reduceOnly")? && !bool_field(&row, "closePosition")? {
            return Err(AccountHostValidationError::RiskEvidence);
        }
    }
    Ok(notionals)
}

fn account_rules(
    catalogue: &str,
    native: &str,
    generation: u64,
) -> Result<BinanceInstrumentRules, AccountHostValidationError> {
    let rules = parse_native_instrument_rules(catalogue, native, generation)
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    Ok(rules)
}

fn quote_asset(rules: &BinanceInstrumentRules) -> Result<Asset, AccountHostValidationError> {
    Asset::new(rules.instrument.symbol.quote())
        .map_err(|_| AccountHostValidationError::RiskEvidence)
}

fn validate_quantity(
    rules: &BinanceInstrumentRules,
    quantity: Decimal,
) -> Result<(), AccountHostValidationError> {
    if quantity <= Decimal::ZERO
        || quantity < rules.minimum_quantity
        || quantity % rules.instrument.quantity_step != Decimal::ZERO
    {
        return Err(AccountHostValidationError::RiskEvidence);
    }
    Ok(())
}

fn json_rows(
    payload: &[u8],
) -> Result<Vec<serde_json::Map<String, Value>>, AccountHostValidationError> {
    let value: Value =
        serde_json::from_slice(payload).map_err(|_| AccountHostValidationError::RiskEvidence)?;
    value
        .as_array()
        .ok_or(AccountHostValidationError::RiskEvidence)?
        .iter()
        .map(|row| {
            row.as_object()
                .cloned()
                .ok_or(AccountHostValidationError::RiskEvidence)
        })
        .collect()
}

fn text_field<'a>(
    row: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, AccountHostValidationError> {
    row.get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(AccountHostValidationError::RiskEvidence)
}

fn decimal_field(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<Decimal, AccountHostValidationError> {
    text_field(row, field)?
        .parse()
        .map_err(|_| AccountHostValidationError::RiskEvidence)
}

fn bool_field(
    row: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<bool, AccountHostValidationError> {
    row.get(field)
        .and_then(Value::as_bool)
        .ok_or(AccountHostValidationError::RiskEvidence)
}

fn now_ms() -> Result<u64, BinanceAccountGatewayError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceAccountGatewayError::Clock)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| BinanceAccountGatewayError::Clock)
}

fn rejected(reason: &str) -> AccountGatewayResult {
    AccountGatewayResult::Rejected {
        reason: reason.to_owned(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BinanceAccountGatewayError {
    #[error("Binance account binding is invalid")]
    Binding,
    #[error("Binance credentials are unavailable")]
    Credentials,
    #[error("Binance account gateway runtime could not start")]
    Runtime,
    #[error("Binance account gateway clock is invalid")]
    Clock,
    #[error("Binance transport failed: {0}")]
    Transport(BinanceTransportError),
    #[error("Binance instrument rules are invalid")]
    Instrument,
    #[error("Binance selected instrument rules changed during this resident generation")]
    RulesChanged,
    #[error("Binance signed private readback is incomplete")]
    Readback,
    #[error("Binance attempt identity exhausted")]
    Attempt,
    #[error("Binance private stream evidence is invalid or unavailable")]
    PrivateStream,
    #[error("Binance public stream evidence is invalid or unavailable")]
    PublicStream,
}

impl From<BinanceTransportError> for BinanceAccountGatewayError {
    fn from(value: BinanceTransportError) -> Self {
        Self::Transport(value)
    }
}

#[cfg(test)]
#[path = "account_gateway_tests.rs"]
mod tests;
