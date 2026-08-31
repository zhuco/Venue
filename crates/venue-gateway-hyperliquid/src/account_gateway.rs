use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use crate::protocol::account::{
    FillCollection, PerpUniverse, parse_account_orders, parse_account_state,
    parse_account_twap_fills, parse_universe,
};
use rust_decimal::Decimal;
use sha3::{Digest, Keccak256};
use tokio::runtime::{Builder, Runtime};
use venue_domain::domain::{
    Asset, ExecutionCommand, FieldState, InstrumentIdentity, LimitTimeInForce, MarketKind,
    MarketReduceCommand, NativeOrderFamily, OrderCommand, OrderSide, OrderState, Position,
    PositionSide, Price,
};
use venue_execution::{
    AccountDispatchPermit, AccountGatewayResult, AccountHostValidationError,
    AccountInstrumentIdentity, AccountLimitNormalizationIntent, AccountPhysicalGateway,
    AccountPricedLimitIntent, AccountRecoveryOutcome, AccountRecoveryReport,
    AccountRecoveryRequest, AccountRecoveryState, AccountRiskAmount, AccountRiskEvidence,
    SignedAccountBalance, SignedAccountOrderFact, SignedAccountPositionFact,
    SignedAccountPositionMode, SignedAccountSnapshot, SignedUnknownFact, SignedUnknownResult,
};
use venue_gateway_api::GatewayBinding;

use crate::action::{
    HyperliquidAloOrder, HyperliquidCancel, HyperliquidExchangeConvergence,
    HyperliquidExchangeOutcome, HyperliquidGtcOrder, HyperliquidIocReduceOnlyOrder,
    begin_exchange_readback, build_alo_place_request, build_cancel_request,
    build_gtc_place_request, build_ioc_reduce_only_request, parse_exchange_ack,
};
use crate::{
    HYPERLIQUID_FILL_RESPONSE_LIMIT, HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT, HyperliquidBbo,
    HyperliquidCredentials, HyperliquidError, HyperliquidGatewayBinding, HyperliquidHttpTransport,
    HyperliquidNonceStore, HyperliquidOrderLookup, HyperliquidOrderStatus, HyperliquidPerpMeta,
    HyperliquidPrivateStreamBinding, HyperliquidPrivateWsTransport, HyperliquidReadBinding,
    HyperliquidTransportError, NonceCheckpoint, build_clearinghouse_state_request,
    build_frontend_open_orders_request, build_l2_book_request, build_meta_request,
    build_order_status_request, build_user_fills_by_time_request,
    build_user_twap_slice_fills_request, parse_frontend_open_orders_snapshot, parse_l2_book_bbo,
    parse_order_status, parse_perp_meta, reserve_next_nonce,
};

const NONCE_CHECKPOINT_MAX_BYTES: u64 = 4 * 1024;
const ACTION_EXPIRY_MS: u64 = 30_000;
const IOC_REDUCE_SLIPPAGE_BPS: u64 = 50;
const BPS_DENOMINATOR: u64 = 10_000;
/// A synchronous `/info` BBO is only usable as a maker price while its exchange timestamp is
/// still contemporaneous with the response.  Receipt time is evidence of transport order, not a
/// substitute for the venue timestamp.
const MAX_LIMIT_BBO_AGE_MS: u64 = 2_000;
const MAX_VISIBLE_FILL_PAGES: usize =
    HYPERLIQUID_RECENT_FILL_RETENTION_LIMIT / HYPERLIQUID_FILL_RESPONSE_LIMIT + 1;
const MAX_RISK_RATE_AGE_MS: u64 = 60_000;

/// Production Hyperliquid adapter for the lightweight account host. The only mutation method is
/// the host trait's linear-permit consumer; raw signing and `/exchange` POST remain crate-private.
pub struct HyperliquidAccountGateway {
    runtime: Runtime,
    binding: HyperliquidReadBinding,
    credentials: HyperliquidCredentials,
    transport: HyperliquidHttpTransport,
    meta: HyperliquidPerpMeta,
    account_safety: AccountSafety,
    nonce_store: FileNonceStore,
    connection_generation: u64,
    snapshot_generation: u64,
}

impl HyperliquidAccountGateway {
    /// Performs meta, clearinghouse, and complete frontend-open-order reads. Construction does not
    /// sign or send an exchange action.
    pub fn connect_from_environment(
        binding: GatewayBinding,
        nonce_checkpoint_path: impl Into<PathBuf>,
        operation_timeout: Duration,
        max_body_bytes: usize,
    ) -> Result<Self, HyperliquidAccountGatewayError> {
        let gateway = HyperliquidGatewayBinding::new(binding)
            .map_err(|_| HyperliquidAccountGatewayError::Binding)?;
        let credentials = HyperliquidCredentials::from_environment()
            .map_err(|_| HyperliquidAccountGatewayError::Credentials)?;
        let read_binding = HyperliquidReadBinding::new(gateway, credentials.user_address())
            .map_err(|_| HyperliquidAccountGatewayError::Binding)?;
        let nonce_store = FileNonceStore::new(
            nonce_checkpoint_path.into(),
            read_binding.gateway().gateway_binding(),
        )?;
        let transport = HyperliquidHttpTransport::new(operation_timeout, max_body_bytes)
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let runtime = Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|_| HyperliquidAccountGatewayError::Runtime)?;
        let meta = runtime.block_on(fetch_meta(&read_binding, &transport))?;
        let account_safety = runtime.block_on(refresh_account(&meta, &transport))?;
        Ok(Self {
            runtime,
            binding: read_binding,
            credentials,
            transport,
            meta,
            account_safety,
            nonce_store,
            connection_generation: unix_ms()?,
            snapshot_generation: 0,
        })
    }

    fn refresh(&mut self) -> Result<(), HyperliquidAccountGatewayError> {
        self.account_safety = self
            .runtime
            .block_on(refresh_account(&self.meta, &self.transport))?;
        Ok(())
    }

    fn refresh_meta(&mut self) -> Result<(), HyperliquidAccountGatewayError> {
        let refreshed = self
            .runtime
            .block_on(fetch_meta(&self.binding, &self.transport))?;
        if refreshed != self.meta {
            return Err(HyperliquidAccountGatewayError::Instrument);
        }
        Ok(())
    }

    fn current_bbo(&mut self) -> Result<HyperliquidBbo, HyperliquidAccountGatewayError> {
        let request = build_l2_book_request(&self.meta)
            .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
        let response = self
            .runtime
            .block_on(self.transport.post_info(&self.binding, &request))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let bbo = parse_l2_book_bbo(&response.body, &self.meta)
            .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
        if bbo.exchange_time_ms > response.received_at_ms {
            return Err(HyperliquidAccountGatewayError::Instrument);
        }
        Ok(bbo)
    }

    fn verify_account_scope(&self) -> Result<(), HyperliquidAccountGatewayError> {
        let (account, user, vault, api_wallet) = self.credentials.public_binding();
        if user != self.binding.user_address()
            || vault.is_some_and(|value| value != user)
            || vault.is_none() && account != user
            || api_wallet == account
            || api_wallet == user
            || vault.is_some_and(|value| value == api_wallet)
            // Credentials derives and compares the public address during construction. Parsing
            // again here prevents a corrupted in-memory key from being used as scope evidence.
            || self.credentials.signing_key().is_err()
        {
            return Err(HyperliquidAccountGatewayError::Binding);
        }
        Ok(())
    }

    fn require_complete_order_coverage(&mut self) -> Result<(), HyperliquidAccountGatewayError> {
        self.verify_account_scope()?;
        let stream_binding = HyperliquidPrivateStreamBinding::new(&self.meta, unix_ms()?)
            .map_err(|_| HyperliquidAccountGatewayError::Binding)?;
        let twap = self
            .runtime
            .block_on(HyperliquidPrivateWsTransport::collect_twap_states(
                stream_binding.clone(),
                Duration::from_secs(10),
                2 * 1024 * 1024,
            ))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let mids = self
            .runtime
            .block_on(HyperliquidPrivateWsTransport::collect_all_mids(
                stream_binding,
                Duration::from_secs(10),
                2 * 1024 * 1024,
            ))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        for parent in twap.states().iter().filter(|parent| !parent.reduce_only) {
            let remaining = parent
                .quantity
                .checked_sub(parent.executed_quantity)
                .filter(|value| *value >= rust_decimal::Decimal::ZERO)
                .ok_or(HyperliquidAccountGatewayError::Account)?;
            let mark = mids
                .get(&parent.coin)
                .ok_or(HyperliquidAccountGatewayError::Account)?;
            let _notional = remaining
                .checked_mul(*mark)
                .ok_or(HyperliquidAccountGatewayError::Account)?;
        }
        let request = build_frontend_open_orders_request(&self.meta)
            .map_err(|_| HyperliquidAccountGatewayError::Account)?;
        let response = self
            .runtime
            .block_on(self.transport.post_info(&self.binding, &request))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let orders = parse_frontend_open_orders_snapshot(
            &response.body,
            &self.meta,
            response.received_at_ms,
        )
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
        if !matches!(orders.regular_coverage, crate::HyperliquidOrderFamilyCoverage::CompleteFrontendSnapshot)
            || !matches!(orders.conditional_coverage, crate::HyperliquidOrderFamilyCoverage::CompleteFrontendSnapshot)
            // The parent set is collected on a separate bounded socket. Any active parent still
            // requires account-wide mark/rule normalization before it can enter risk evidence.
            || !matches!(orders.algo_coverage, crate::HyperliquidOrderFamilyCoverage::NotCoveredByFrontendOpenOrders)
            || !twap.states().is_empty()
        {
            return Err(HyperliquidAccountGatewayError::IncompleteOrderCoverage);
        }
        Ok(())
    }

    fn order_status(
        &mut self,
        lookup: &HyperliquidOrderLookup,
    ) -> Result<HyperliquidOrderStatus, HyperliquidAccountGatewayError> {
        let request = build_order_status_request(&self.meta, lookup)
            .map_err(|_| HyperliquidAccountGatewayError::Readback)?;
        let response = self
            .runtime
            .block_on(self.transport.post_info(&self.binding, &request))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        parse_order_status(&response.body, &self.meta, lookup)
            .map_err(|_| HyperliquidAccountGatewayError::Readback)
    }

    fn reserve_nonce(&mut self) -> Result<crate::PersistedNonce, HyperliquidAccountGatewayError> {
        let now_ms = unix_ms()?;
        reserve_next_nonce(
            &mut self.nonce_store,
            self.credentials.api_wallet_address(),
            now_ms,
        )
        .map_err(|_| HyperliquidAccountGatewayError::Nonce)
    }

    fn dispatch_permit(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        if permit.binding() != self.binding.gateway().gateway_binding() {
            return rejected("hyperliquid_permit_binding");
        }
        if self.refresh_meta().is_err() || self.refresh().is_err() {
            return rejected("hyperliquid_preflight_failed");
        }
        let now_ms = match unix_ms() {
            Ok(value) => value,
            Err(_) => return rejected("hyperliquid_clock"),
        };
        let expires_after_ms = match now_ms.checked_add(ACTION_EXPIRY_MS) {
            Some(value) => Some(value),
            None => return rejected("hyperliquid_clock"),
        };
        let request = match permit.command() {
            ExecutionCommand::PlaceLimit(command) => {
                if !command.reduce_only
                    && (self.account_safety.has_position || self.account_safety.has_open_orders)
                {
                    return rejected("hyperliquid_existing_account_risk");
                }
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let cloid = command_cloid(command.client_order_id.as_str());
                match command.time_in_force {
                    LimitTimeInForce::PostOnly => {
                        let order = match HyperliquidAloOrder::new(
                            &self.meta,
                            command.side,
                            command.limit_price.value(),
                            command.quantity,
                            command.reduce_only,
                            cloid,
                        ) {
                            Ok(value) => value,
                            Err(_) => return rejected("hyperliquid_intent_rejected"),
                        };
                        build_alo_place_request(&self.credentials, nonce, order, expires_after_ms)
                    }
                    LimitTimeInForce::Gtc => {
                        let order = match HyperliquidGtcOrder::new(
                            &self.meta,
                            command.side,
                            command.limit_price.value(),
                            command.quantity,
                            command.reduce_only,
                            cloid,
                        ) {
                            Ok(value) => value,
                            Err(_) => return rejected("hyperliquid_intent_rejected"),
                        };
                        build_gtc_place_request(&self.credentials, nonce, order, expires_after_ms)
                    }
                }
            }
            ExecutionCommand::Cancel(command) => {
                let lookup = match HyperliquidOrderLookup::client_order_id(command_cloid(
                    command.target_client_order_id.as_str(),
                )) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_cancel_identity"),
                };
                let order_id = match self.order_status(&lookup) {
                    Ok(HyperliquidOrderStatus::Known { order_id, .. }) => order_id,
                    Ok(HyperliquidOrderStatus::Unknown { .. }) | Err(_) => {
                        return rejected("hyperliquid_cancel_target_unresolved");
                    }
                };
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let cancel = match HyperliquidCancel::new(&self.meta, order_id) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_cancel_identity"),
                };
                build_cancel_request(&self.credentials, nonce, cancel, expires_after_ms)
            }
            ExecutionCommand::MarketReduce(command) => {
                let position = match self.account_safety.position.as_ref() {
                    Some(value) => value,
                    None => return rejected("hyperliquid_market_reduce_position"),
                };
                if validate_market_reduce_position(command, position).is_err() {
                    return rejected("hyperliquid_market_reduce_position");
                }
                let bbo = match self.current_bbo() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_reduce_rules"),
                };
                let price = match ioc_reduce_price(command.side, &bbo, &self.meta) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_reduce_rules"),
                };
                let nonce = match self.reserve_nonce() {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_nonce"),
                };
                let order = match HyperliquidIocReduceOnlyOrder::new(
                    &self.meta,
                    command.side,
                    price,
                    command.quantity,
                    command_cloid(command.client_order_id.as_str()),
                ) {
                    Ok(value) => value,
                    Err(_) => return rejected("hyperliquid_market_reduce_rules"),
                };
                build_ioc_reduce_only_request(&self.credentials, nonce, order, expires_after_ms)
            }
            ExecutionCommand::PlaceMarket(_)
            | ExecutionCommand::StopMarketCloseAll(_)
            | ExecutionCommand::StopMarketFullPosition(_) => {
                return rejected("hyperliquid_initial_profile_unsupported_command");
            }
        };
        let request = match request {
            Ok(value) => value,
            Err(_) => return rejected("hyperliquid_signing_rejected"),
        };
        match self
            .runtime
            .block_on(self.transport.post_exchange(request.binding(), &request))
        {
            Ok(response) => match parse_exchange_ack(&response.body, &request) {
                Ok(outcome @ HyperliquidExchangeOutcome::Resting { .. })
                | Ok(outcome @ HyperliquidExchangeOutcome::Filled { .. })
                | Ok(outcome @ HyperliquidExchangeOutcome::Cancelled { .. }) => {
                    self.confirm_exchange_readback(&request, &outcome)
                }
                Ok(HyperliquidExchangeOutcome::Rejected { reason }) => {
                    AccountGatewayResult::Rejected { reason }
                }
                Err(_) => AccountGatewayResult::Unknown,
            },
            Err(error) => map_transport_dispatch(error),
        }
    }

    fn confirm_exchange_readback(
        &mut self,
        request: &crate::action::HyperliquidExchangeRequest,
        acknowledgement: &HyperliquidExchangeOutcome,
    ) -> AccountGatewayResult {
        let generation = match unix_ms() {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let private = match HyperliquidPrivateStreamBinding::new(&self.meta, generation) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let plan = match begin_exchange_readback(request, Some(acknowledgement), &private) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        let status = match self.order_status(plan.lookup()) {
            Ok(value) => value,
            Err(_) => return AccountGatewayResult::Unknown,
        };
        match plan.reconcile(Some(&status)) {
            Ok(HyperliquidExchangeConvergence::Confirmed { order_id, .. }) => {
                AccountGatewayResult::Accepted {
                    venue_order_id: order_id.to_string(),
                }
            }
            Ok(HyperliquidExchangeConvergence::Rejected { reason }) => {
                AccountGatewayResult::Rejected { reason }
            }
            Ok(HyperliquidExchangeConvergence::PendingUnknown) | Err(_) => {
                AccountGatewayResult::Unknown
            }
        }
    }

    fn collect_visible_fills(
        &mut self,
        previous_cursor: Option<&str>,
        universe: &PerpUniverse,
        end_ms: u64,
    ) -> Result<(Vec<venue_domain::domain::Fill>, String), HyperliquidAccountGatewayError> {
        let mut collection = FillCollection::resume(previous_cursor, &self.meta, end_ms)
            .map_err(|_| HyperliquidAccountGatewayError::Account)?;
        for _ in 0..MAX_VISIBLE_FILL_PAGES {
            let query = collection
                .query(&self.meta)
                .map_err(|_| HyperliquidAccountGatewayError::Account)?;
            let request = build_user_fills_by_time_request(&query)
                .map_err(|_| HyperliquidAccountGatewayError::Account)?;
            let response = self
                .runtime
                .block_on(self.transport.post_info(&self.binding, &request))
                .map_err(HyperliquidAccountGatewayError::Transport)?;
            if collection
                .ingest(&response.body, &self.meta, universe)
                .map_err(|_| HyperliquidAccountGatewayError::Account)?
            {
                return collection
                    .finish()
                    .map_err(|_| HyperliquidAccountGatewayError::Account);
            }
        }
        Err(HyperliquidAccountGatewayError::Account)
    }

    fn collect_twap_slice_fills(
        &mut self,
        universe: &PerpUniverse,
    ) -> Result<Vec<venue_domain::domain::Fill>, HyperliquidAccountGatewayError> {
        let request = build_user_twap_slice_fills_request(&self.meta)
            .map_err(|_| HyperliquidAccountGatewayError::Account)?;
        let response = self
            .runtime
            .block_on(self.transport.post_info(&self.binding, &request))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let rows = parse_account_twap_fills(&response.body, universe)
            .map_err(|_| HyperliquidAccountGatewayError::Account)?;
        if rows.iter().any(|row| {
            row.exchange_time_ms
                .is_none_or(|time_ms| time_ms > response.received_at_ms)
        }) {
            return Err(HyperliquidAccountGatewayError::Account);
        }
        Ok(rows)
    }

    fn collect_signed_snapshot(
        &mut self,
        previous_cursor: Option<&str>,
    ) -> Result<HyperliquidSignedSnapshot, HyperliquidAccountGatewayError> {
        let collection_started_ms = unix_ms()?;
        self.verify_account_scope()?;
        let meta_request = build_meta_request(&self.binding)
            .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
        let meta_response = self
            .runtime
            .block_on(self.transport.post_info(&self.binding, &meta_request))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let universe = parse_universe(&meta_response.body, &self.binding)
            .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
        let selected_meta = universe
            .get(self.binding.gateway().gateway_binding().symbol.base())
            .cloned()
            .ok_or(HyperliquidAccountGatewayError::Instrument)?;
        if selected_meta != self.meta {
            return Err(HyperliquidAccountGatewayError::Instrument);
        }
        let account_request = build_clearinghouse_state_request(&self.meta)
            .map_err(|_| HyperliquidAccountGatewayError::Account)?;
        let account_response = self
            .runtime
            .block_on(self.transport.post_info(&self.binding, &account_request))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let account = parse_account_state(&account_response.body, &universe, &self.meta)
            .map_err(|_| HyperliquidAccountGatewayError::Account)?;
        if account.exchange_time_ms > account_response.received_at_ms {
            return Err(HyperliquidAccountGatewayError::Account);
        }
        let orders_request = build_frontend_open_orders_request(&self.meta)
            .map_err(|_| HyperliquidAccountGatewayError::Account)?;
        let orders_response = self
            .runtime
            .block_on(self.transport.post_info(&self.binding, &orders_request))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let orders = parse_account_orders(
            &orders_response.body,
            &universe,
            orders_response.received_at_ms,
        )
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
        let generation = unix_ms()?;
        let private = HyperliquidPrivateStreamBinding::new(&self.meta, generation)
            .map_err(|_| HyperliquidAccountGatewayError::Binding)?;
        let twap = self
            .runtime
            .block_on(HyperliquidPrivateWsTransport::collect_twap_states(
                private.clone(),
                Duration::from_secs(10),
                2 * 1024 * 1024,
            ))
            .map_err(HyperliquidAccountGatewayError::Transport)?;
        let twap_fills = self.collect_twap_slice_fills(&universe)?;
        let twap_orders = if twap.states().is_empty() {
            Vec::new()
        } else {
            // A TWAP parent has no bounded resting limit.  Record its unfilled balance with a
            // contemporaneous mark solely for account-risk accounting; it remains an external
            // UmAlgo fact and is never eligible for ownership or mutation routing.
            let mids = self
                .runtime
                .block_on(HyperliquidPrivateWsTransport::collect_all_mids(
                    private,
                    Duration::from_secs(10),
                    2 * 1024 * 1024,
                ))
                .map_err(HyperliquidAccountGatewayError::Transport)?;
            signed_twap_order_facts(&twap, &mids, &self.meta)?
        };
        let observed_at_ms = unix_ms()?;
        let (mut fills, fills_cursor) =
            self.collect_visible_fills(previous_cursor, &universe, observed_at_ms)?;
        let mut known_fills = BTreeMap::new();
        for fill in fills.drain(..).chain(twap_fills) {
            match known_fills.insert(fill.fill_id.clone(), fill.clone()) {
                Some(previous) if previous != fill => {
                    return Err(HyperliquidAccountGatewayError::Account);
                }
                _ => {}
            }
        }
        let mut orders = signed_order_facts(&orders)?;
        orders.extend(twap_orders);
        Ok(HyperliquidSignedSnapshot {
            // Later slow fills/TWAP requests cannot make an older account read look fresh.
            observed_at_ms: collection_started_ms.min(account.exchange_time_ms),
            orders,
            positions: account.positions,
            balance: account.balance,
            fills: known_fills.into_values().collect(),
            fills_cursor,
        })
    }
}

impl AccountPhysicalGateway for HyperliquidAccountGateway {
    type Error = HyperliquidAccountGatewayError;

    fn binding(&self) -> &GatewayBinding {
        self.binding.gateway().gateway_binding()
    }

    fn signed_client_order_id_matches(
        &self,
        canonical: &venue_domain::CommandId,
        signed: &str,
    ) -> bool {
        command_cloid(canonical.as_str()) == signed
    }

    fn current_instrument(
        &mut self,
    ) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
        // `refresh_meta` reads current native metadata and rejects drift against this resident's
        // rules generation. The documented price rule is dynamic, so only expose the identity
        // Copy actually consumes; execution retains `price_wire` as its authoritative validator.
        self.refresh_meta()
            .map_err(|_| AccountHostValidationError::Instrument)?;
        copy_rules_identity(&self.meta, self.connection_generation)
    }

    fn reconcile(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<AccountRecoveryReport, Self::Error> {
        if request.binding() != self.binding.gateway().gateway_binding() {
            return Err(HyperliquidAccountGatewayError::Binding);
        }
        self.refresh()?;
        let observed_at_ms = unix_ms()?;
        let mut outcomes = Vec::with_capacity(request.unresolved().len());
        for command in request.unresolved() {
            let client_id = match command {
                ExecutionCommand::Cancel(cancel) => cancel.target_client_order_id.as_str(),
                _ => command
                    .native_client_id()
                    .ok_or(HyperliquidAccountGatewayError::Readback)?
                    .as_str(),
            };
            let lookup = HyperliquidOrderLookup::client_order_id(command_cloid(client_id))
                .map_err(|_| HyperliquidAccountGatewayError::Readback)?;
            outcomes.push(hyperliquid_recovery_status_outcome(
                command,
                self.order_status(&lookup),
            ));
        }
        AccountRecoveryReport::new(
            self.binding.gateway().gateway_binding().clone(),
            observed_at_ms,
            outcomes,
        )
        .map_err(|_| HyperliquidAccountGatewayError::Readback)
    }

    fn risk_evidence(&mut self) -> Result<AccountRiskEvidence, AccountHostValidationError> {
        let started_at_ms = unix_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?;
        self.require_complete_order_coverage()
            .map_err(|_| AccountHostValidationError::RiskEvidence)?;
        self.runtime.block_on(fetch_account_wide_risk(
            &self.binding,
            &self.transport,
            started_at_ms,
        ))
    }

    fn signed_account_snapshot(
        &mut self,
        request: &AccountRecoveryRequest,
    ) -> Result<SignedAccountSnapshot, AccountHostValidationError> {
        if request.binding() != self.binding.gateway().gateway_binding() {
            return Err(AccountHostValidationError::SignedSnapshot);
        }
        let collected = self
            .collect_signed_snapshot(request.previous_fills_cursor())
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
        let private_generation = self
            .snapshot_generation
            .checked_add(1)
            .ok_or(AccountHostValidationError::SignedSnapshot)?;
        let snapshot = SignedAccountSnapshot::complete_with_fills(
            self.binding.gateway().gateway_binding().clone(),
            collected.observed_at_ms,
            self.connection_generation,
            private_generation,
            self.connection_generation,
            SignedAccountPositionMode::Net,
            collected.orders,
            collected.positions,
            collected.fills,
            collected.fills_cursor,
            unknown_results,
        )?
        .with_balances(vec![collected.balance])?;
        self.snapshot_generation = private_generation;
        Ok(snapshot)
    }

    fn normalize_limit_intent(
        &mut self,
        intent: &AccountLimitNormalizationIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        validate_limit_intent_scope(intent, self.binding.gateway().gateway_binding())?;
        // Rules and BBO are fetched per intent.  The cached meta cannot authorize a price or
        // quantity after a contract's precision or trading status changed.
        self.refresh_meta()
            .map_err(|_| AccountHostValidationError::Command)?;
        let bbo = self
            .current_bbo()
            .map_err(|_| AccountHostValidationError::Command)?;
        normalize_limit_from_bbo(
            intent,
            &self.meta,
            &bbo,
            unix_ms().map_err(|_| AccountHostValidationError::Command)?,
        )
    }

    fn normalize_priced_limit_intent(
        &mut self,
        intent: &AccountPricedLimitIntent,
    ) -> Result<ExecutionCommand, AccountHostValidationError> {
        self.refresh_meta()
            .map_err(|_| AccountHostValidationError::Command)?;
        normalize_priced_limit(intent, &self.meta)
    }

    fn dispatch(&mut self, permit: AccountDispatchPermit) -> AccountGatewayResult {
        self.dispatch_permit(permit)
    }
}

fn copy_rules_identity(
    meta: &HyperliquidPerpMeta,
    rules_generation: u64,
) -> Result<AccountInstrumentIdentity, AccountHostValidationError> {
    let binding = meta.scope.binding().gateway().gateway_binding();
    if rules_generation == 0 || !meta.trading_enabled || binding.symbol.quote() != "USDC" {
        return Err(AccountHostValidationError::Instrument);
    }
    let settlement =
        Asset::new(binding.symbol.quote()).map_err(|_| AccountHostValidationError::Instrument)?;
    Ok(AccountInstrumentIdentity {
        identity: InstrumentIdentity {
            symbol: binding.symbol.clone(),
            market: MarketKind::LinearPerpetual,
            settlement_asset: Some(settlement),
        },
        rules_generation,
    })
}

async fn fetch_meta(
    binding: &HyperliquidReadBinding,
    transport: &HyperliquidHttpTransport,
) -> Result<HyperliquidPerpMeta, HyperliquidAccountGatewayError> {
    let request =
        build_meta_request(binding).map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
    let response = transport
        .post_info(binding, &request)
        .await
        .map_err(HyperliquidAccountGatewayError::Transport)?;
    let meta = parse_perp_meta(&response.body, binding)
        .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
    if !meta.trading_enabled {
        return Err(HyperliquidAccountGatewayError::Instrument);
    }
    Ok(meta)
}

async fn refresh_account(
    meta: &HyperliquidPerpMeta,
    transport: &HyperliquidHttpTransport,
) -> Result<AccountSafety, HyperliquidAccountGatewayError> {
    let meta_request = build_meta_request(meta.scope.binding())
        .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
    let meta_response = transport
        .post_info(meta.scope.binding(), &meta_request)
        .await
        .map_err(HyperliquidAccountGatewayError::Transport)?;
    let universe = parse_universe(&meta_response.body, meta.scope.binding())
        .map_err(|_| HyperliquidAccountGatewayError::Instrument)?;
    let selected = universe
        .get(meta.scope.native_coin())
        .ok_or(HyperliquidAccountGatewayError::Instrument)?;
    let account_request = build_clearinghouse_state_request(meta)
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
    let account = transport
        .post_info(meta.scope.binding(), &account_request)
        .await
        .map_err(HyperliquidAccountGatewayError::Transport)?;
    let snapshot = parse_account_state(&account.body, &universe, selected)
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
    if snapshot.exchange_time_ms > account.received_at_ms {
        return Err(HyperliquidAccountGatewayError::Account);
    }
    let orders_request = build_frontend_open_orders_request(meta)
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
    let orders = transport
        .post_info(meta.scope.binding(), &orders_request)
        .await
        .map_err(HyperliquidAccountGatewayError::Transport)?;
    let orders = parse_account_orders(&orders.body, &universe, orders.received_at_ms)
        .map_err(|_| HyperliquidAccountGatewayError::Account)?;
    let position = snapshot
        .positions
        .iter()
        .find(|position| position.symbol == *meta.scope.symbol())
        .filter(|position| !position.quantity.is_zero())
        .map(position_from_signed_fact)
        .transpose()?;
    Ok(AccountSafety {
        has_position: snapshot
            .positions
            .iter()
            .any(|position| !position.quantity.is_zero()),
        position,
        has_open_orders: !orders.is_empty(),
    })
}

async fn fetch_account_wide_risk(
    binding: &HyperliquidReadBinding,
    transport: &HyperliquidHttpTransport,
    started_at_ms: u64,
) -> Result<AccountRiskEvidence, AccountHostValidationError> {
    let meta_request =
        build_meta_request(binding).map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let meta_response = transport
        .post_info(binding, &meta_request)
        .await
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let universe = parse_universe(&meta_response.body, binding)
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let selected = universe
        .get(binding.gateway().gateway_binding().symbol.base())
        .ok_or(AccountHostValidationError::RiskEvidence)?;
    let account_request = build_clearinghouse_state_request(selected)
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let account_response = transport
        .post_info(binding, &account_request)
        .await
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let account = parse_account_state(&account_response.body, &universe, selected)
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    if account.exchange_time_ms > account_response.received_at_ms {
        return Err(AccountHostValidationError::RiskEvidence);
    }
    let orders_request = build_frontend_open_orders_request(selected)
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let orders_response = transport
        .post_info(binding, &orders_request)
        .await
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let orders = parse_account_orders(
        &orders_response.body,
        &universe,
        orders_response.received_at_ms,
    )
    .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let (positions, open_entries) = account_risk_amounts(&account.positions, &orders)?;
    let generation = unix_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let rate = transport
        .fetch_usdc_to_usdt_rate(generation, MAX_RISK_RATE_AGE_MS)
        .await
        .map_err(|_| AccountHostValidationError::RiskEvidence)?;
    AccountRiskEvidence::complete_with_usdt_valuation(
        binding.gateway().gateway_binding().clone(),
        unix_ms().map_err(|_| AccountHostValidationError::RiskEvidence)?,
        generation,
        positions,
        open_entries,
        vec![rate],
    )?
    .with_earliest_observation(started_at_ms.min(account.exchange_time_ms))
}

fn account_risk_amounts(
    positions: &[SignedAccountPositionFact],
    orders: &[crate::HyperliquidOpenOrder],
) -> Result<(Vec<AccountRiskAmount>, Vec<AccountRiskAmount>), AccountHostValidationError> {
    let usdc = Asset::new("USDC").map_err(|_| AccountHostValidationError::RiskEvidence)?;
    let positions = positions
        .iter()
        .filter(|position| !position.quantity.is_zero())
        .map(|position| {
            let mark = position
                .mark_price
                .filter(|mark| *mark > Decimal::ZERO)
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            Ok(AccountRiskAmount {
                asset: usdc.clone(),
                value: position
                    .quantity
                    .abs()
                    .checked_mul(mark)
                    .ok_or(AccountHostValidationError::Notional)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let entries = orders
        .iter()
        .filter(|item| !item.order.reduce_only)
        .map(|item| {
            let price = item
                .order
                .limit_price
                .map(Price::value)
                .filter(|price| *price > Decimal::ZERO)
                .ok_or(AccountHostValidationError::RiskEvidence)?;
            if !item.order.quantity.is_sign_positive() {
                return Err(AccountHostValidationError::RiskEvidence);
            }
            Ok(AccountRiskAmount {
                asset: usdc.clone(),
                value: item
                    .order
                    .quantity
                    .checked_mul(price)
                    .ok_or(AccountHostValidationError::Notional)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((positions, entries))
}

fn position_from_signed_fact(
    fact: &SignedAccountPositionFact,
) -> Result<Position, HyperliquidAccountGatewayError> {
    let side = if fact.quantity > Decimal::ZERO {
        PositionSide::Long
    } else {
        PositionSide::Short
    };
    Ok(Position {
        symbol: fact.symbol.clone(),
        side,
        quantity: fact.quantity.abs(),
        entry_price: fact
            .entry_price
            .map(Price::new)
            .transpose()
            .map_err(|_| HyperliquidAccountGatewayError::Account)?,
        mark_price: fact
            .mark_price
            .map(Price::new)
            .transpose()
            .map_err(|_| HyperliquidAccountGatewayError::Account)?,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AccountSafety {
    has_position: bool,
    position: Option<Position>,
    has_open_orders: bool,
}

struct HyperliquidSignedSnapshot {
    observed_at_ms: u64,
    orders: Vec<SignedAccountOrderFact>,
    positions: Vec<SignedAccountPositionFact>,
    balance: SignedAccountBalance,
    fills: Vec<venue_domain::domain::Fill>,
    fills_cursor: String,
}

fn signed_order_facts(
    orders: &[crate::HyperliquidOpenOrder],
) -> Result<Vec<SignedAccountOrderFact>, HyperliquidAccountGatewayError> {
    let mut facts = Vec::with_capacity(orders.len());
    for item in orders {
        let order = &item.order;
        let client_order_id = match &order.client_order_id {
            venue_domain::domain::FieldState::Known(value) => value.clone(),
            venue_domain::domain::FieldState::Missing
            | venue_domain::domain::FieldState::Null
            | venue_domain::domain::FieldState::Unavailable { .. }
            | venue_domain::domain::FieldState::NotApplicable => order.order_id.clone(),
        };
        if client_order_id.trim().is_empty() || !order.quantity.is_sign_positive() {
            return Err(HyperliquidAccountGatewayError::Account);
        }
        let time_in_force = match item.family {
            crate::HyperliquidOrderFamily::Regular => match order.time_in_force {
                FieldState::Known(value) => Some(value),
                FieldState::Missing
                | FieldState::Null
                | FieldState::Unavailable { .. }
                | FieldState::NotApplicable => return Err(HyperliquidAccountGatewayError::Account),
            },
            crate::HyperliquidOrderFamily::Conditional => None,
        };
        facts.push(SignedAccountOrderFact {
            client_order_id,
            venue_order_id: Some(order.order_id.clone()),
            symbol: order.symbol.clone(),
            family: match item.family {
                crate::HyperliquidOrderFamily::Regular => NativeOrderFamily::UmOrder,
                crate::HyperliquidOrderFamily::Conditional => NativeOrderFamily::UmConditional,
            },
            side: order.side,
            position_side: PositionSide::Net,
            quantity: order.quantity,
            state: Some(order.state),
            filled_quantity: Some(order.filled_quantity),
            limit_price: order.limit_price.map(|price| price.value()),
            time_in_force,
            reduce_only: order.reduce_only,
            owner: None,
            external: true,
            // `timestamp` is the native order creation time in the frontend-open-orders schema.
            created_at_ms: Some(item.exchange_time_ms),
        });
    }
    Ok(facts)
}

fn signed_twap_order_facts(
    snapshot: &crate::HyperliquidTwapStatesSnapshot,
    marks: &BTreeMap<String, Decimal>,
    meta: &HyperliquidPerpMeta,
) -> Result<Vec<SignedAccountOrderFact>, HyperliquidAccountGatewayError> {
    let mut facts = Vec::with_capacity(snapshot.states().len());
    for parent in snapshot.states() {
        let remaining = parent
            .quantity
            .checked_sub(parent.executed_quantity)
            .filter(|value| *value >= Decimal::ZERO)
            .ok_or(HyperliquidAccountGatewayError::Account)?;
        if remaining.is_zero() {
            continue;
        }
        let mark = *marks
            .get(&parent.coin)
            .filter(|value| **value > Decimal::ZERO)
            .ok_or(HyperliquidAccountGatewayError::Account)?;
        remaining
            .checked_mul(mark)
            .ok_or(HyperliquidAccountGatewayError::Account)?;
        let symbol = if parent.coin == meta.scope.native_coin() {
            meta.scope.symbol().clone()
        } else {
            venue_domain::domain::Symbol::new(&parent.coin, "USDC")
                .map_err(|_| HyperliquidAccountGatewayError::Account)?
        };
        facts.push(SignedAccountOrderFact {
            client_order_id: format!("hl-twap-{}", parent.twap_id),
            venue_order_id: Some(parent.twap_id.to_string()),
            symbol,
            family: NativeOrderFamily::UmAlgo,
            side: parent.side,
            position_side: PositionSide::Net,
            quantity: remaining,
            state: None,
            filled_quantity: None,
            // The TWAP parent has no resting limit. Its mark is only a checked valuation
            // input above and must never be published as an exchange limit order price.
            limit_price: None,
            time_in_force: None,
            reduce_only: parent.reduce_only,
            owner: None,
            external: true,
            created_at_ms: None,
        });
    }
    Ok(facts)
}

fn validate_limit_intent_scope(
    intent: &AccountLimitNormalizationIntent,
    binding: &GatewayBinding,
) -> Result<(), AccountHostValidationError> {
    // The shared intent currently validates USDT only, whereas Hyperliquid perpetuals settle in
    // USDC.  Keep that asset exception local to the adapter instead of weakening other venues.
    if intent.quote_delta <= Decimal::ZERO
        || intent.owner.symbol.quote() != "USDC"
        || intent.owner.validate().is_err()
        || intent.owner.exchange != binding.venue.as_str()
        || intent.owner.account != binding.trading_account_id
        || intent.owner.symbol != binding.symbol
    {
        return Err(AccountHostValidationError::Scope);
    }
    if intent.position_side == PositionSide::Net
        || !valid_hedge_limit_direction(intent.position_side, intent.side, intent.reduce_only)
    {
        return Err(AccountHostValidationError::Command);
    }
    Ok(())
}

fn normalize_limit_from_bbo(
    intent: &AccountLimitNormalizationIntent,
    meta: &HyperliquidPerpMeta,
    bbo: &HyperliquidBbo,
    now_ms: u64,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    validate_limit_intent_scope(intent, meta.scope.binding().gateway().gateway_binding())?;
    if bbo.scope != meta.scope
        || bbo.exchange_time_ms == 0
        || bbo.exchange_time_ms > now_ms
        || now_ms - bbo.exchange_time_ms > MAX_LIMIT_BBO_AGE_MS
        || bbo.bid.price >= bbo.ask.price
    {
        return Err(AccountHostValidationError::Command);
    }
    // ALO orders must remain outside the spread: buy against the best bid, sell against the best
    // ask.  Validating through the native action builder enforces the current Hyperliquid price
    // scale and significant-digit contract without duplicating its wire rules here.
    let limit_price = match intent.side {
        OrderSide::Buy => bbo.bid.price,
        OrderSide::Sell => bbo.ask.price,
    };
    let quantity_step = Decimal::new(1, meta.size_decimals);
    let quantity = intent
        .quote_delta
        .checked_div(limit_price.value())
        .and_then(|value| value.checked_div(quantity_step))
        .map(|value| value.floor())
        .and_then(|units| units.checked_mul(quantity_step))
        .filter(|value| *value >= quantity_step)
        .ok_or(AccountHostValidationError::Command)?;
    HyperliquidAloOrder::new(
        meta,
        intent.side,
        limit_price.value(),
        quantity,
        intent.reduce_only,
        command_cloid(intent.client_order_id.as_str()),
    )
    .map_err(|_| AccountHostValidationError::Command)?;
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        time_in_force: Default::default(),
        command_id: intent.command_id.clone(),
        client_order_id: intent.client_order_id.clone(),
        owner: intent.owner.clone(),
        side: intent.side,
        position_side: intent.position_side,
        quantity,
        limit_price: Price::new(limit_price.value())
            .map_err(|_| AccountHostValidationError::Command)?,
        reduce_only: intent.reduce_only,
    }))
}

fn normalize_priced_limit(
    priced: &AccountPricedLimitIntent,
    meta: &HyperliquidPerpMeta,
) -> Result<ExecutionCommand, AccountHostValidationError> {
    let intent = &priced.intent;
    validate_limit_intent_scope(intent, meta.scope.binding().gateway().gateway_binding())?;
    priced.validate()?;
    let quantity_step = Decimal::new(1, meta.size_decimals);
    let quantity = priced
        .quantity_cap()?
        .checked_div(quantity_step)
        .map(|value| value.floor())
        .and_then(|units| units.checked_mul(quantity_step))
        .filter(|value| *value >= quantity_step)
        .ok_or(AccountHostValidationError::Command)?;
    let notional = quantity
        .checked_mul(priced.limit_price.value())
        .ok_or(AccountHostValidationError::Notional)?;
    if notional > intent.quote_delta {
        return Err(AccountHostValidationError::Command);
    }
    match priced.time_in_force {
        LimitTimeInForce::PostOnly => {
            HyperliquidAloOrder::new(
                meta,
                intent.side,
                priced.limit_price.value(),
                quantity,
                intent.reduce_only,
                command_cloid(intent.client_order_id.as_str()),
            )
            .map_err(|_| AccountHostValidationError::Command)?;
        }
        LimitTimeInForce::Gtc => {
            HyperliquidGtcOrder::new(
                meta,
                intent.side,
                priced.limit_price.value(),
                quantity,
                intent.reduce_only,
                command_cloid(intent.client_order_id.as_str()),
            )
            .map_err(|_| AccountHostValidationError::Command)?;
        }
    }
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        time_in_force: priced.time_in_force,
        command_id: intent.command_id.clone(),
        client_order_id: intent.client_order_id.clone(),
        owner: intent.owner.clone(),
        side: intent.side,
        position_side: intent.position_side,
        quantity,
        limit_price: priced.limit_price,
        reduce_only: intent.reduce_only,
    }))
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

fn validate_market_reduce_position(
    command: &MarketReduceCommand,
    position: &Position,
) -> Result<(), ()> {
    command
        .validate_with_authoritative_position(position)
        .map_err(|_| ())?;
    if position.quantity.is_zero() || command.quantity > position.quantity {
        return Err(());
    }
    Ok(())
}

/// Hyperliquid receives a marketable IOC limit, not a separate market wire type.  The protected
/// side is moved by 50 bps, then rounded only in the marketable direction until it fits the
/// venue's size-decimal/price-significance contract.
fn ioc_reduce_price(
    side: OrderSide,
    bbo: &HyperliquidBbo,
    meta: &HyperliquidPerpMeta,
) -> Result<Decimal, ()> {
    let denominator = Decimal::from(BPS_DENOMINATOR);
    let adjustment = Decimal::from(IOC_REDUCE_SLIPPAGE_BPS);
    let raw = match side {
        OrderSide::Sell => bbo
            .bid
            .price
            .value()
            .checked_mul(denominator.checked_sub(adjustment).ok_or(())?)
            .and_then(|value| value.checked_div(denominator))
            .ok_or(())?,
        OrderSide::Buy => bbo
            .ask
            .price
            .value()
            .checked_mul(denominator.checked_add(adjustment).ok_or(())?)
            .and_then(|value| value.checked_div(denominator))
            .ok_or(())?,
    };
    let max_scale = 6_u32.checked_sub(meta.size_decimals).ok_or(())?;
    for scale in (0..=max_scale).rev() {
        let factor = Decimal::from(10_u64.pow(scale));
        let scaled = raw.checked_mul(factor).ok_or(())?;
        let truncated = scaled.floor();
        let units = match side {
            OrderSide::Sell => truncated,
            OrderSide::Buy if scaled == truncated => truncated,
            OrderSide::Buy => truncated.checked_add(Decimal::ONE).ok_or(())?,
        };
        let candidate = units.checked_div(factor).ok_or(())?;
        if candidate > Decimal::ZERO
            && HyperliquidIocReduceOnlyOrder::new(
                meta,
                side,
                candidate,
                Decimal::ONE,
                "0x00000000000000000000000000000001",
            )
            .is_ok()
        {
            return Ok(candidate);
        }
    }
    Err(())
}

fn hyperliquid_recovery_outcome(
    command: &ExecutionCommand,
    status: HyperliquidOrderStatus,
) -> AccountRecoveryOutcome {
    match status {
        HyperliquidOrderStatus::Unknown { .. } => {
            AccountRecoveryOutcome::still_unknown(command.command_id().clone())
        }
        status @ HyperliquidOrderStatus::Known { .. }
            if !hyperliquid_recovery_policy_matches(command, &status) =>
        {
            AccountRecoveryOutcome::still_unknown(command.command_id().clone())
        }
        HyperliquidOrderStatus::Known {
            order_id, state, ..
        } if matches!(command, ExecutionCommand::Cancel(_)) => match state {
            OrderState::Cancelled => {
                AccountRecoveryOutcome::accepted(command.command_id().clone(), order_id.to_string())
            }
            OrderState::Filled | OrderState::Expired | OrderState::Rejected => {
                AccountRecoveryOutcome::rejected(
                    command.command_id().clone(),
                    "hyperliquid_target_terminal_without_cancel".to_owned(),
                )
            }
            _ => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
        },
        HyperliquidOrderStatus::Known {
            order_id: _,
            state: OrderState::Rejected,
            ..
        } => AccountRecoveryOutcome::rejected(
            command.command_id().clone(),
            "hyperliquid_order_rejected".to_owned(),
        ),
        HyperliquidOrderStatus::Known { order_id, .. } => {
            AccountRecoveryOutcome::accepted(command.command_id().clone(), order_id.to_string())
        }
    }
}

fn hyperliquid_recovery_status_outcome(
    command: &ExecutionCommand,
    status: Result<HyperliquidOrderStatus, HyperliquidAccountGatewayError>,
) -> AccountRecoveryOutcome {
    match status {
        Ok(status) => hyperliquid_recovery_outcome(command, status),
        Err(_) => AccountRecoveryOutcome::still_unknown(command.command_id().clone()),
    }
}

fn hyperliquid_recovery_policy_matches(
    command: &ExecutionCommand,
    status: &HyperliquidOrderStatus,
) -> bool {
    match (command, status) {
        (
            ExecutionCommand::PlaceLimit(command),
            HyperliquidOrderStatus::Known {
                native_order_type,
                time_in_force,
                ..
            },
        ) => {
            native_order_type == "Limit"
                && matches!(
                    (command.time_in_force, time_in_force.as_deref()),
                    (LimitTimeInForce::PostOnly, Some("Alo"))
                        | (LimitTimeInForce::Gtc, Some("Gtc"))
                )
        }
        (ExecutionCommand::PlaceLimit(_), HyperliquidOrderStatus::Unknown { .. }) => false,
        _ => true,
    }
}

fn command_cloid(command_id: &str) -> String {
    let mut digest = Keccak256::new();
    digest.update(b"venue-hyperliquid-command-cloid-v1");
    digest.update(command_id.as_bytes());
    let digest = digest.finalize();
    let mut value = String::with_capacity(34);
    value.push_str("0x");
    for byte in &digest[..16] {
        value.push_str(&format!("{byte:02x}"));
    }
    value
}

fn map_transport_dispatch(error: HyperliquidTransportError) -> AccountGatewayResult {
    match error {
        HyperliquidTransportError::Configuration
        | HyperliquidTransportError::Binding
        | HyperliquidTransportError::BodyTooLarge
        | HyperliquidTransportError::Protocol
        | HyperliquidTransportError::Clock => rejected("hyperliquid_pre_send_rejected"),
        _ => AccountGatewayResult::Unknown,
    }
}

fn rejected(reason: &str) -> AccountGatewayResult {
    AccountGatewayResult::Rejected {
        reason: reason.to_owned(),
    }
}

fn unix_ms() -> Result<u64, HyperliquidAccountGatewayError> {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| HyperliquidAccountGatewayError::Clock)?
        .as_millis();
    u64::try_from(millis).map_err(|_| HyperliquidAccountGatewayError::Clock)
}

struct FileNonceStore {
    path: PathBuf,
}

impl FileNonceStore {
    fn new(
        path: PathBuf,
        binding: &GatewayBinding,
    ) -> Result<Self, HyperliquidAccountGatewayError> {
        if !path.is_absolute()
            || path.file_name().and_then(|value| value.to_str()) != Some("nonce.json")
            || !path.parent().is_some_and(|parent| {
                parent.ends_with(
                    Path::new(binding.venue.as_str())
                        .join(binding.mode.as_str())
                        .join(&binding.trading_account_id),
                )
            })
        {
            return Err(HyperliquidAccountGatewayError::NoncePath);
        }
        Ok(Self { path })
    }
}

impl HyperliquidNonceStore for FileNonceStore {
    fn load(&mut self, _agent_address: &str) -> Result<Option<NonceCheckpoint>, HyperliquidError> {
        let metadata = match fs::metadata(&self.path) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(HyperliquidError::Nonce),
        };
        if metadata.len() == 0 || metadata.len() > NONCE_CHECKPOINT_MAX_BYTES {
            return Err(HyperliquidError::Nonce);
        }
        let mut file = File::open(&self.path).map_err(|_| HyperliquidError::Nonce)?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.read_to_end(&mut bytes)
            .map_err(|_| HyperliquidError::Nonce)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| HyperliquidError::Nonce)
    }

    fn persist(&mut self, checkpoint: &NonceCheckpoint) -> Result<(), HyperliquidError> {
        let bytes = serde_json::to_vec(checkpoint).map_err(|_| HyperliquidError::Nonce)?;
        if bytes.is_empty() || bytes.len() as u64 > NONCE_CHECKPOINT_MAX_BYTES {
            return Err(HyperliquidError::Nonce);
        }
        let parent = self.path.parent().ok_or(HyperliquidError::Nonce)?;
        fs::create_dir_all(parent).map_err(|_| HyperliquidError::Nonce)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.path)
            .map_err(|_| HyperliquidError::Nonce)?;
        file.write_all(&bytes)
            .map_err(|_| HyperliquidError::Nonce)?;
        file.sync_all().map_err(|_| HyperliquidError::Nonce)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HyperliquidAccountGatewayError {
    #[error("Hyperliquid account gateway binding is invalid")]
    Binding,
    #[error("Hyperliquid account gateway credentials are unavailable")]
    Credentials,
    #[error("Hyperliquid account gateway runtime could not be created")]
    Runtime,
    #[error("Hyperliquid public perpetual metadata is unavailable or invalid")]
    Instrument,
    #[error("Hyperliquid account/open-order preflight failed")]
    Account,
    #[error("Hyperliquid signed open-order collection does not cover every order family")]
    IncompleteOrderCoverage,
    #[error("Hyperliquid exact orderStatus readback failed")]
    Readback,
    #[error("Hyperliquid nonce checkpoint path is outside the bound account artifact root")]
    NoncePath,
    #[error("Hyperliquid durable nonce reservation failed")]
    Nonce,
    #[error("Hyperliquid clock is invalid")]
    Clock,
    #[error("Hyperliquid transport failed")]
    Transport(#[source] HyperliquidTransportError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_user_twap_slice_fills;
    use venue_domain::domain::{CommandId, OrderOwner, OrderPurpose, PositionSide};
    use venue_gateway_api::{GatewayMode, VenueId};

    const META: &[u8] = include_bytes!("../fixtures/perp-meta.json");
    const BOOK: &[u8] = include_bytes!("../fixtures/l2-book.json");
    const IOC_CONTRACT: &str = include_str!("../fixtures/market-reduce-ioc-contract.json");
    const LIMIT_CONTRACT: &str = include_str!("../fixtures/limit-normalization-contract.json");
    const FRONTEND_ORDERS: &[u8] = include_bytes!("../fixtures/frontend-open-orders-family.json");
    const TWAP_SLICE_FILLS: &str = include_str!("../fixtures/twap-slice-fills.json");

    fn market_facts() -> Result<(HyperliquidPerpMeta, HyperliquidBbo), Box<dyn std::error::Error>> {
        let gateway = HyperliquidGatewayBinding::new(GatewayBinding::new(
            VenueId::Hyperliquid,
            GatewayMode::Live,
            "00000000-0000-4000-8000-000000000001",
            "BTC/USDC".parse()?,
        )?)?;
        let binding =
            HyperliquidReadBinding::new(gateway, "0x0000000000000000000000000000000000000001")?;
        let meta = parse_perp_meta(META, &binding)?;
        let bbo = parse_l2_book_bbo(BOOK, &meta)?;
        Ok((meta, bbo))
    }

    #[test]
    fn fresh_meta_maps_to_copy_identity_without_inventing_a_static_tick()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, _) = market_facts()?;
        let identity = copy_rules_identity(&meta, 17)?;
        assert_eq!(identity.rules_generation, 17);
        assert_eq!(identity.identity.symbol, "BTC/USDC".parse()?);
        assert_eq!(identity.identity.market, MarketKind::LinearPerpetual);
        assert_eq!(
            identity.identity.settlement_asset,
            Some(Asset::new("USDC")?)
        );

        let mut delisted = meta;
        delisted.trading_enabled = false;
        assert_eq!(
            copy_rules_identity(&delisted, 17),
            Err(AccountHostValidationError::Instrument)
        );
        assert_eq!(
            copy_rules_identity(&delisted, 0),
            Err(AccountHostValidationError::Instrument)
        );
        Ok(())
    }

    fn reduce(
        side: OrderSide,
        quantity: Decimal,
    ) -> Result<MarketReduceCommand, Box<dyn std::error::Error>> {
        Ok(MarketReduceCommand {
            command_id: CommandId::new("hyper_reduce")?,
            client_order_id: CommandId::new("hyper_reduce_client")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "hyperliquid".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDC".parse()?,
                purpose: OrderPurpose::ExposureTakeProfit,
            },
            position_side: PositionSide::Long,
            side,
            quantity,
            risk_episode_id: CommandId::new("hyper_episode")?,
            position_generation: 3,
        })
    }

    fn limit_intent(
        side: OrderSide,
        position_side: PositionSide,
        reduce_only: bool,
        quote_delta: Decimal,
    ) -> Result<AccountLimitNormalizationIntent, Box<dyn std::error::Error>> {
        Ok(AccountLimitNormalizationIntent {
            command_id: CommandId::new("hyper_limit")?,
            client_order_id: CommandId::new("hyper_limit_client")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "hyperliquid".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDC".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side,
            position_side,
            quote_delta,
            reduce_only,
        })
    }

    #[test]
    fn cloid_is_stable_lower_hex_and_exact_wire_length() {
        let first = command_cloid("client-order-1");
        assert_eq!(first, command_cloid("client-order-1"));
        assert_ne!(first, command_cloid("client-order-2"));
        assert_eq!(first.len(), 34);
        assert!(first.starts_with("0x"));
        assert!(first[2..].bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[test]
    fn market_reduce_uses_fixture_ioc_slippage_prices_and_native_precision()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, bbo) = market_facts()?;
        let fixture: serde_json::Value = serde_json::from_str(IOC_CONTRACT)?;
        let sell_price = ioc_reduce_price(OrderSide::Sell, &bbo, &meta)
            .map_err(|_| std::io::Error::other("sell ioc price"))?;
        let buy_price = ioc_reduce_price(OrderSide::Buy, &bbo, &meta)
            .map_err(|_| std::io::Error::other("buy ioc price"))?;
        assert_eq!(
            IOC_REDUCE_SLIPPAGE_BPS,
            fixture["slippageBps"].as_u64().unwrap_or_default()
        );
        assert_eq!(
            sell_price.to_string(),
            fixture["sellPrice"].as_str().unwrap_or_default()
        );
        assert_eq!(
            buy_price.to_string(),
            fixture["buyPrice"].as_str().unwrap_or_default()
        );
        let order = HyperliquidIocReduceOnlyOrder::new(
            &meta,
            OrderSide::Sell,
            sell_price,
            Decimal::new(1, 2),
            "0x00000000000000000000000000000001",
        )?;
        assert_eq!(order.scope(), &meta.scope);
        Ok(())
    }

    #[test]
    fn market_reduce_rejects_wrong_direction_flat_and_crossing_positions()
    -> Result<(), Box<dyn std::error::Error>> {
        let position = Position {
            symbol: "BTC/USDC".parse()?,
            side: PositionSide::Long,
            quantity: Decimal::ONE,
            entry_price: None,
            mark_price: None,
        };
        assert!(
            validate_market_reduce_position(&reduce(OrderSide::Sell, Decimal::ONE)?, &position)
                .is_ok()
        );
        assert!(
            validate_market_reduce_position(
                &reduce(OrderSide::Sell, Decimal::new(1001, 3))?,
                &position
            )
            .is_err()
        );
        assert!(
            validate_market_reduce_position(&reduce(OrderSide::Buy, Decimal::ONE)?, &position)
                .is_err()
        );
        let mut flat = position;
        flat.quantity = Decimal::ZERO;
        assert!(
            validate_market_reduce_position(&reduce(OrderSide::Sell, Decimal::ONE)?, &flat)
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn unknown_market_reduce_status_stays_unknown_for_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, _) = market_facts()?;
        let lookup = HyperliquidOrderLookup::client_order_id(command_cloid("hyper_reduce_client"))?;
        let status = parse_order_status(br#"{"status":"unknownOid"}"#, &meta, &lookup)?;
        let outcome = hyperliquid_recovery_outcome(
            &ExecutionCommand::MarketReduce(reduce(OrderSide::Sell, Decimal::ONE)?),
            status,
        );
        assert!(matches!(
            outcome.state(),
            venue_execution::AccountRecoveryState::StillUnknown
        ));
        Ok(())
    }

    #[test]
    fn limit_recovery_policy_mismatch_or_missing_stays_unknown()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, _) = market_facts()?;
        let command = ExecutionCommand::PlaceLimit(OrderCommand {
            time_in_force: LimitTimeInForce::Gtc,
            command_id: CommandId::new("hyper_policy")?,
            client_order_id: CommandId::new("hyper_policy_client")?,
            owner: OrderOwner {
                strategy_instance_id: "grid1".to_owned(),
                run_id: "run1".to_owned(),
                exchange: "hyperliquid".to_owned(),
                account: "00000000-0000-4000-8000-000000000001".to_owned(),
                symbol: "BTC/USDC".parse()?,
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(4, 1),
            limit_price: Price::new(Decimal::new(6_500_500, 3))?,
            reduce_only: false,
        });
        let lookup = HyperliquidOrderLookup::client_order_id(command_cloid("hyper_policy_client"))?;
        let payload = serde_json::json!({
            "status":"order",
            "order":{"order":{
                "children":[], "coin":"BTC", "isPositionTpsl":false,
                "isTrigger":false, "side":"B", "limitPx":"6500.5", "sz":"0.4",
                "oid":77, "timestamp":1_700_000_000_001_u64, "reduceOnly":false,
                "orderType":"Limit", "origSz":"0.4", "tif":"Alo",
                "triggerCondition":"N/A", "triggerPx":"0.0",
                "cloid":command_cloid("hyper_policy_client")
            }, "status":"open", "statusTimestamp":1_700_000_000_002_u64}
        });
        let mismatched = parse_order_status(&serde_json::to_vec(&payload)?, &meta, &lookup)?;
        assert!(matches!(
            hyperliquid_recovery_outcome(&command, mismatched).state(),
            venue_execution::AccountRecoveryState::StillUnknown
        ));
        assert!(matches!(
            hyperliquid_recovery_status_outcome(
                &command,
                Err(HyperliquidAccountGatewayError::Readback)
            )
            .state(),
            venue_execution::AccountRecoveryState::StillUnknown
        ));
        Ok(())
    }

    #[test]
    fn priced_limit_keeps_user_price_policy_and_cap() -> Result<(), Box<dyn std::error::Error>> {
        let (meta, bbo) = market_facts()?;
        let priced = AccountPricedLimitIntent {
            intent: limit_intent(OrderSide::Buy, PositionSide::Long, false, Decimal::from(10))?,
            limit_price: bbo.bid.price,
            time_in_force: LimitTimeInForce::Gtc,
            maximum_quantity: Some(Decimal::new(1, 1)),
        };
        let command = normalize_priced_limit(&priced, &meta)?;
        let ExecutionCommand::PlaceLimit(order) = command else {
            return Err("not limit".into());
        };
        assert_eq!(order.limit_price, priced.limit_price);
        assert_eq!(order.time_in_force, LimitTimeInForce::Gtc);
        assert!(order.quantity <= Decimal::new(1, 1));

        let mut noncanonical = priced;
        noncanonical.limit_price = Price::new(Decimal::new(650_001_2, 2))?;
        assert!(normalize_priced_limit(&noncanonical, &meta).is_err());
        Ok(())
    }

    #[test]
    fn signed_order_fact_uses_native_frontend_creation_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, _) = market_facts()?;
        let orders =
            parse_frontend_open_orders_snapshot(FRONTEND_ORDERS, &meta, 1_700_000_000_010)?;
        let facts = signed_order_facts(&orders.orders)?;
        assert_eq!(facts.len(), orders.orders.len());
        assert_eq!(
            facts[0].created_at_ms,
            Some(orders.orders[0].exchange_time_ms)
        );
        Ok(())
    }

    #[test]
    fn limit_normalizer_uses_fresh_bbo_floors_size_and_preserves_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, bbo) = market_facts()?;
        let fixture: serde_json::Value = serde_json::from_str(LIMIT_CONTRACT)?;
        let intent = limit_intent(OrderSide::Buy, PositionSide::Long, false, Decimal::from(10))?;
        let command = normalize_limit_from_bbo(
            &intent,
            &meta,
            &bbo,
            bbo.exchange_time_ms.checked_add(1).ok_or("time")?,
        )?;
        let ExecutionCommand::PlaceLimit(order) = command else {
            return Err("expected place-limit".into());
        };
        assert_eq!(order.command_id, intent.command_id);
        assert_eq!(order.client_order_id, intent.client_order_id);
        assert_eq!(order.owner, intent.owner);
        assert_eq!(
            order.limit_price.value().to_string(),
            fixture["buyPrice"].as_str().unwrap_or_default()
        );
        assert_eq!(
            order.quantity.to_string(),
            fixture["buyQuantity"].as_str().unwrap_or_default()
        );
        assert!(!order.reduce_only);
        Ok(())
    }

    #[test]
    fn account_risk_marks_every_nonzero_net_position_in_usdc()
    -> Result<(), Box<dyn std::error::Error>> {
        let fact = SignedAccountPositionFact {
            symbol: "ETH/USDC".parse()?,
            position_side: PositionSide::Net,
            quantity: Decimal::new(-25, 1),
            entry_price: Some(Decimal::from(100)),
            mark_price: Some(Decimal::from(120)),
        };
        let (positions, entries) = account_risk_amounts(std::slice::from_ref(&fact), &[])?;
        assert_eq!(entries.len(), 0);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].asset.as_str(), "USDC");
        assert_eq!(positions[0].value, Decimal::from(300));
        let missing_mark = SignedAccountPositionFact {
            mark_price: None,
            ..fact
        };
        assert!(account_risk_amounts(&[missing_mark], &[]).is_err());
        Ok(())
    }

    #[test]
    fn limit_normalizer_uses_ask_for_sell_and_native_alo_contract()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, bbo) = market_facts()?;
        let fixture: serde_json::Value = serde_json::from_str(LIMIT_CONTRACT)?;
        let intent = limit_intent(
            OrderSide::Sell,
            PositionSide::Short,
            false,
            Decimal::from(10),
        )?;
        let command = normalize_limit_from_bbo(
            &intent,
            &meta,
            &bbo,
            bbo.exchange_time_ms.checked_add(1).ok_or("time")?,
        )?;
        let ExecutionCommand::PlaceLimit(order) = command else {
            return Err("expected place-limit".into());
        };
        assert_eq!(
            order.limit_price.value().to_string(),
            fixture["sellPrice"].as_str().unwrap_or_default()
        );
        assert_eq!(
            order.quantity.to_string(),
            fixture["sellQuantity"].as_str().unwrap_or_default()
        );
        let alo = HyperliquidAloOrder::new(
            &meta,
            order.side,
            order.limit_price.value(),
            order.quantity,
            order.reduce_only,
            command_cloid(order.client_order_id.as_str()),
        )?;
        assert_eq!(alo.scope(), &meta.scope);
        Ok(())
    }

    #[test]
    fn limit_normalizer_rejects_stale_scope_direction_and_sub_step_quantity()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, bbo) = market_facts()?;
        let valid = limit_intent(OrderSide::Buy, PositionSide::Long, false, Decimal::from(10))?;
        assert_eq!(
            normalize_limit_from_bbo(
                &valid,
                &meta,
                &bbo,
                bbo.exchange_time_ms
                    .checked_add(MAX_LIMIT_BBO_AGE_MS)
                    .and_then(|value| value.checked_add(1))
                    .ok_or("time")?,
            ),
            Err(AccountHostValidationError::Command)
        );
        let wrong_direction =
            limit_intent(OrderSide::Buy, PositionSide::Long, true, Decimal::from(10))?;
        assert_eq!(
            normalize_limit_from_bbo(
                &wrong_direction,
                &meta,
                &bbo,
                bbo.exchange_time_ms.checked_add(1).ok_or("time")?,
            ),
            Err(AccountHostValidationError::Command)
        );
        let too_small = limit_intent(
            OrderSide::Buy,
            PositionSide::Long,
            false,
            Decimal::new(1, 2),
        )?;
        assert_eq!(
            normalize_limit_from_bbo(
                &too_small,
                &meta,
                &bbo,
                bbo.exchange_time_ms.checked_add(1).ok_or("time")?,
            ),
            Err(AccountHostValidationError::Command)
        );
        let mut wrong_scope = valid.clone();
        wrong_scope.owner.account = "00000000-0000-4000-8000-000000000002".to_owned();
        assert_eq!(
            normalize_limit_from_bbo(
                &wrong_scope,
                &meta,
                &bbo,
                bbo.exchange_time_ms.checked_add(1).ok_or("time")?,
            ),
            Err(AccountHostValidationError::Scope)
        );
        let mut crossed = bbo.clone();
        crossed.bid.price = crossed.ask.price;
        assert_eq!(
            normalize_limit_from_bbo(
                &valid,
                &meta,
                &crossed,
                bbo.exchange_time_ms.checked_add(1).ok_or("time")?,
            ),
            Err(AccountHostValidationError::Command)
        );
        let mut unrepresentable = bbo;
        unrepresentable.bid.price = Price::new(Decimal::new(1_133_771, 1))?;
        assert_eq!(
            normalize_limit_from_bbo(
                &valid,
                &meta,
                &unrepresentable,
                unrepresentable
                    .exchange_time_ms
                    .checked_add(1)
                    .ok_or("time")?,
            ),
            Err(AccountHostValidationError::Command)
        );
        Ok(())
    }

    #[test]
    fn twap_slice_fills_are_normalized_and_capped_or_conflicting_pages_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, _) = market_facts()?;
        let request = build_user_twap_slice_fills_request(&meta)?;
        let request_body: serde_json::Value = serde_json::from_slice(request.body())?;
        assert_eq!(request_body["type"], "userTwapSliceFills");
        assert_eq!(request_body["user"], meta.scope.user_address());
        let parsed = parse_user_twap_slice_fills(TWAP_SLICE_FILLS.as_bytes(), &meta)?;
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].twap_id, 9);
        assert_eq!(parsed[0].fill.fill.symbol, "BTC/USDC".parse()?);

        let mut duplicate: serde_json::Value = serde_json::from_str(TWAP_SLICE_FILLS)?;
        let first = duplicate
            .as_array()
            .and_then(|rows| rows.first())
            .cloned()
            .ok_or("fill")?;
        duplicate.as_array_mut().ok_or("array")?.push(first);
        assert!(parse_user_twap_slice_fills(duplicate.to_string().as_bytes(), &meta).is_err());

        let row = serde_json::from_str::<serde_json::Value>(TWAP_SLICE_FILLS)?
            .as_array()
            .and_then(|rows| rows.first())
            .cloned()
            .ok_or("fill")?;
        let capped = serde_json::to_vec(&vec![row; HYPERLIQUID_FILL_RESPONSE_LIMIT])?;
        assert!(parse_user_twap_slice_fills(&capped, &meta).is_err());
        Ok(())
    }

    #[test]
    fn active_twap_parent_is_an_external_algo_fact_with_marked_remaining_risk()
    -> Result<(), Box<dyn std::error::Error>> {
        let (meta, _) = market_facts()?;
        let private = HyperliquidPrivateStreamBinding::new(&meta, 7)?;
        let payload = br#"{"channel":"twapStates","data":{"dex":"","user":"0x0000000000000000000000000000000000000001","states":[[9,{"coin":"BTC","user":"0x0000000000000000000000000000000000000001","side":"B","sz":"2","executedSz":"0.5","executedNtl":"30000","reduceOnly":false,"timestamp":1700000000000}]]}}"#;
        let states = crate::parse_twap_states_snapshot(payload, &private)?;
        let marks = BTreeMap::from([("BTC".to_owned(), Decimal::from(60_000))]);
        let facts = signed_twap_order_facts(&states, &marks, &meta)?;
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].family, NativeOrderFamily::UmAlgo);
        assert_eq!(facts[0].quantity, Decimal::new(15, 1));
        assert_eq!(facts[0].limit_price, None);
        assert_eq!(facts[0].created_at_ms, None);
        assert!(facts[0].external);
        Ok(())
    }
}
