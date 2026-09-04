//! Testable signed-execution boundary. Production wiring will adapt this trait to the existing
//! Binance transport; mocks exercise idempotency and readback without a network path.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use venue_domain::domain::{
    FieldState, LimitTimeInForce, OrderSide, OrderState, PositionSide, Symbol,
};
use venue_gateway_binance::{BinanceAccountGateway, BinanceCredentials};
use venue_gateway_binance::{
    BinanceCancelIntent, BinanceHttpTransport, BinanceMarketIntent, BinancePhysicalMutationOutcome,
    BinancePlaceIntent, BinancePrivateReadScope, BinanceTimeInForce, BinanceTransportError,
    build_account_config_request, build_account_request, build_algo_orders_request,
    build_exact_order_request, build_fills_request, build_position_mode_request,
    build_positions_request, build_regular_orders_request, complete_private_readback,
    parse_instrument_rules, prepare_cancel, prepare_place_limit, prepare_place_market,
    private::{RecentFillsCursor, parse_order},
};
use venue_gateway_binance::{GatewayBinding, GatewayMode, VenueId};

#[path = "executor_exchange/grid_batch.rs"]
mod grid_batch;
use grid_batch::{elapsed_us, record_outbound_timing, validate_grid_batch_shape};
mod catalogue;
mod terminal_open;
use catalogue::SharedCatalogue;
pub(crate) use terminal_open::is_terminal_open;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionRequest {
    pub origin: venue_control_protocol::kol::ExecutorCommandOrigin,
    pub command_id: String,
    pub client_order_id: String,
    pub credential_id: String,
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub order_kind: ExecutionOrderKind,
    pub known_native_order_id: Option<String>,
    pub reconciled_close_reservations: Vec<ReconciledCloseReservation>,
}

/// A maker close which the command ledger has completed but the latest central signed projection
/// predates. The direct signed surface suppresses this reservation as soon as it sees the client
/// ID; a later central projection removes it from subsequent requests even if the order terminated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciledCloseReservation {
    pub credential_id: String,
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub client_order_id: String,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    pub reconciled_ms: u64,
    pub projection_observed_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionOrderKind {
    Market {
        side: OrderSide,
        position_side: PositionSide,
        quantity: Decimal,
        reducing: bool,
    },
    LimitPostOnly {
        side: OrderSide,
        position_side: PositionSide,
        quantity: Decimal,
        price: Decimal,
        reducing: bool,
    },
    CancelExact {
        native_order_id: Option<String>,
        target_client_order_id: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionReadback {
    Accepted,
    Reconciled,
    Rejected,
    Unknown,
}

/// A state classification plus the exact signed exchange identity that supports it. An ACK-only
/// identity is retained while the state remains Unknown; it never upgrades to Accepted by itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionOutcome {
    pub exchange_error_code: Option<i64>,
    pub state: ExecutionReadback,
    pub native_order_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GridBatchCommandOutcome {
    Submitted(ExecutionOutcome),
    NotDispatched(BinanceExecutionError),
}

/// Monotonic executor-local timing. This deliberately does not call itself event latency: the
/// authenticated-event to durable-commit/wake interval is measured by the Grid producer, while
/// these fields measure router entry through local validation/preflight to send-entry. The runtime
/// combines them with the durable authenticated-event timestamp for end-to-end telemetry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GridBatchSubmitTiming {
    pub executor_start_to_first_submit_us: Option<u64>,
    pub executor_start_to_last_submit_us: Option<u64>,
    pub first_to_last_submit_us: Option<u64>,
    pub outbound_attempts: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridBatchExecutionOutcome {
    pub commands: Vec<GridBatchCommandOutcome>,
    pub timing: GridBatchSubmitTiming,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GridBatchExecutionContext {
    pub batch_id: String,
    pub owner_user_id: String,
    pub durable: Option<crate::kol_executor::GridBatchDispatchContext>,
}

/// A batch-level failure must state whether any physical mutation may have entered transport.
/// Per-command outcomes remain preferred after dispatch starts; this fallback exists so an
/// adapter failure can never be mistaken for proof that the complete batch was not sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GridBatchSubmitError {
    #[error("Binance Grid batch failed before every physical mutation dispatch")]
    DefinitelyNotDispatched(BinanceExecutionError),
    #[error("Binance Grid batch dispatch may have started and requires signed reconciliation")]
    DispatchUncertain,
}

/// Signed account baseline outcome used before a Pending activation can become Active. `Clean`
/// means the adapter checked permissions, balance, hedge positions and both ordinary/Algo order
/// surfaces; it is intentionally not inferred from a credential's prior verification record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountBaseline {
    Clean,
    Blocked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BinanceExecutionError {
    #[error("Binance execution is unavailable")]
    Unavailable,
    #[error("Binance execution request is invalid")]
    Invalid,
    #[error("Manual opening quantity rounds down to zero")]
    OpenQuantityZero,
}

impl BinanceExecutionError {
    /// `submit` only returns these errors before calling the physical mutation transport. Once a
    /// POST is attempted, uncertainty is represented by `ExecutionReadback::Unknown` instead.
    #[must_use]
    pub const fn not_dispatched_code(self) -> &'static str {
        match self {
            Self::Invalid => "not_dispatched_invalid",
            Self::OpenQuantityZero => "not_dispatched_quantity_zero",
            Self::Unavailable => "not_dispatched_unavailable",
        }
    }
}

/// The production Portfolio Margin UM adapter. It owns no credentials and never retries a
/// mutation: a durable command ID can pass through `submit` once, while later calls only use
/// `readback`. The supplied transport is the existing signed HTTP implementation, so fixtures
/// can exercise the exact same request/signature surface without a second client.
pub struct BinanceHttpExecution {
    transport: BinanceHttpTransport,
    catalogue: SharedCatalogue,
    fills_cursor: Option<RecentFillsCursor>,
    next_attempt_id: u64,
}

/// Routes one singleton's durable commands to an account-and-symbol-bound HTTP adapter. The
/// map only retains transport connection state; PostgreSQL remains the command authority.
#[derive(Clone)]
pub struct BinanceExecutionRouter {
    exchanges:
        Arc<Mutex<BTreeMap<(String, Symbol), Arc<tokio::sync::Mutex<BinanceHttpExecution>>>>>,
    limits: venue_gateway_binance::BinanceTransportLimits,
    hot_dispatch: crate::GridHotDispatchCache,
    catalogue: SharedCatalogue,
}

impl BinanceExecutionRouter {
    #[must_use]
    pub fn new(limits: venue_gateway_binance::BinanceTransportLimits) -> Self {
        Self::with_hot_dispatch(limits, crate::GridHotDispatchCache::new())
    }

    #[must_use]
    pub fn with_hot_dispatch(
        limits: venue_gateway_binance::BinanceTransportLimits,
        hot_dispatch: crate::GridHotDispatchCache,
    ) -> Self {
        Self {
            exchanges: Arc::new(Mutex::new(BTreeMap::new())),
            limits,
            hot_dispatch,
            catalogue: SharedCatalogue::default(),
        }
    }

    fn take_matching_hot_token(
        &self,
        context: &GridBatchExecutionContext,
        requests: &[ExecutionRequest],
    ) -> Option<crate::GridHotDispatchToken> {
        // Take first so every mismatch, including a superseded private projection, permanently
        // retires the one-shot acceleration token before the signed cold preflight begins.
        let token = self.hot_dispatch.take(&context.batch_id)?;
        let durable = context.durable.as_ref()?;
        let source_event_received_ms = durable.source_event_received_ms?;
        let first = requests.first()?;
        let now = now_ms().ok()?;
        (durable.private_projection_current
            && token.valid()
            && token.batch_id == context.batch_id
            && token.batch_digest == durable.batch_digest
            && token.owner_user_id == context.owner_user_id
            && token.trading_account_id == first.trading_account_id
            && token.credential_id == first.credential_id
            && token.symbol == first.symbol
            && token.private_generation == durable.private_generation
            && token.private_observed_ms == durable.private_observed_ms
            && token.rules.instrument.generation == durable.instrument_generation
            && token.source_event_received_ms == source_event_received_ms
            && source_event_received_ms <= now
            && now <= token.valid_until_ms)
            .then_some(token)
    }

    fn exchange(
        &self,
        request: &ExecutionRequest,
    ) -> Result<Arc<tokio::sync::Mutex<BinanceHttpExecution>>, BinanceExecutionError> {
        self.account_exchange(&request.trading_account_id, &request.symbol)
    }

    /// Prime the actual mutation transports before installing a strategy's private baseline.
    /// This reads public server time only; no credentials, account reads or orders are involved.
    pub async fn prepare_account_transports(
        &self,
        trading_account_id: &str,
        symbols: &BTreeSet<Symbol>,
    ) -> Result<(), BinanceExecutionError> {
        for symbol in symbols {
            let exchange = self.account_exchange(trading_account_id, symbol)?;
            let exchange = exchange.lock().await;
            if exchange.transport.signing_timestamp_ms().is_err() {
                exchange
                    .transport
                    .synchronize_clock()
                    .await
                    .map_err(|_| BinanceExecutionError::Unavailable)?;
            }
        }
        Ok(())
    }

    fn account_exchange(
        &self,
        trading_account_id: &str,
        symbol: &Symbol,
    ) -> Result<Arc<tokio::sync::Mutex<BinanceHttpExecution>>, BinanceExecutionError> {
        let key = (trading_account_id.to_owned(), symbol.clone());
        let mut exchanges = self
            .exchanges
            .lock()
            .map_err(|_| BinanceExecutionError::Unavailable)?;
        if !exchanges.contains_key(&key) {
            let binding = GatewayBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                trading_account_id.to_owned(),
                symbol.clone(),
            )
            .map_err(|_| BinanceExecutionError::Invalid)?;
            let config = venue_gateway_binance::BinanceConfig::for_binding(
                venue_gateway_binance::BinanceAccountBinding::PortfolioMarginUm,
                &binding,
            )
            .map_err(|_| BinanceExecutionError::Invalid)?;
            let transport = BinanceHttpTransport::new(config, 1, 1, self.limits)
                .map_err(|_| BinanceExecutionError::Unavailable)?;
            let mut execution = BinanceHttpExecution::new(transport);
            execution.catalogue = self.catalogue.clone();
            exchanges.insert(key.clone(), Arc::new(tokio::sync::Mutex::new(execution)));
        }
        exchanges
            .get_mut(&key)
            .cloned()
            .ok_or(BinanceExecutionError::Unavailable)
    }

    pub async fn maintain_clocks(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut delay = std::time::Duration::from_secs(3_600);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return; }
                }
                _ = tokio::time::sleep(delay) => {
                    let prepared = self.exchanges.lock().ok().map(|exchanges| exchanges.values().cloned().collect::<Vec<_>>());
                    let Some(prepared) = prepared else { return; };
                    let mut complete = true;
                    for exchange in prepared {
                        let refresh = match exchange.try_lock() {
                            Ok(exchange) => exchange.transport.prepare_clock_refresh(),
                            Err(_) => { complete = false; continue; }
                        };
                        tokio::select! {
                            _ = shutdown.changed() => return,
                            result = refresh => {
                                if result.is_err() { complete = false; }
                            }
                        }
                    }
                    tracing::info!(target: "venue_control::grid_hot_path", complete,
                        "Scheduled Binance clock refresh completed outside order execution");
                    delay = clock_refresh_delay(complete);
                }
            }
        }
    }
}

fn clock_refresh_delay(complete: bool) -> std::time::Duration {
    std::time::Duration::from_secs(if complete { 3_600 } else { 60 })
}

impl BinanceHttpExecution {
    #[must_use]
    pub fn new(transport: BinanceHttpTransport) -> Self {
        Self {
            transport,
            catalogue: SharedCatalogue::default(),
            fills_cursor: None,
            next_attempt_id: 1,
        }
    }

    async fn snapshot_with_rules(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
    ) -> Result<
        (
            venue_gateway_binance::BinancePrivateReadbackCandidate,
            venue_gateway_binance::BinanceInstrumentRules,
        ),
        BinanceExecutionError,
    > {
        validate_request_binding(&self.transport, request)?;
        self.transport
            .synchronize_clock()
            .await
            .map_err(|_| BinanceExecutionError::Unavailable)?;
        let exchange_info = self
            .transport
            .fetch_usd_m_exchange_info()
            .await
            .map_err(|_| BinanceExecutionError::Unavailable)?;
        let exchange_info = std::str::from_utf8(&exchange_info.payload)
            .map_err(|_| BinanceExecutionError::Unavailable)?;
        let rules = parse_instrument_rules(
            exchange_info,
            request.symbol.clone(),
            self.transport.instrument_generation(),
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        let now = now_ms()?;
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(BinanceExecutionError::Unavailable)?;
        let scope = BinancePrivateReadScope::new(
            self.transport.config(),
            &rules,
            self.transport.private_generation(),
            attempt_id,
            now,
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        let initial_cursor = self.fills_cursor.unwrap_or(RecentFillsCursor {
            // Execution preflight needs a complete current account/order surface, not historical
            // fill recovery. Starting immediately before this signed snapshot keeps the one-page
            // read complete even for active accounts; durable fill recovery belongs to the
            // central authenticated projection.
            observed_through_ms: now.saturating_sub(1),
            last_trade_id: None,
            last_event_time_ms: None,
        });
        let fills = build_fills_request(
            &scope,
            1,
            initial_cursor,
            initial_cursor.observed_through_ms,
            now,
        )
        .map_err(|_| BinanceExecutionError::Unavailable)?;
        let requests = [
            build_account_request(&scope),
            build_account_config_request(&scope),
            build_position_mode_request(&scope),
            build_regular_orders_request(&scope),
            build_algo_orders_request(&scope),
        ];
        let mut pages = Vec::with_capacity(7);
        for request in requests {
            let request = request.map_err(|_| BinanceExecutionError::Unavailable)?;
            pages.push(
                self.transport
                    .execute_read(credentials, &request, now)
                    .await
                    .map_err(|_| BinanceExecutionError::Unavailable)?,
            );
        }
        pages.push(
            self.transport
                .execute_read(credentials, &fills, now)
                .await
                .map_err(|_| BinanceExecutionError::Unavailable)?,
        );
        let positions =
            build_positions_request(&scope).map_err(|_| BinanceExecutionError::Unavailable)?;
        pages.push(
            self.transport
                .execute_read(credentials, &positions, now)
                .await
                .map_err(|_| BinanceExecutionError::Unavailable)?,
        );
        let candidate = complete_private_readback(
            self.transport.config(),
            &rules,
            &scope,
            initial_cursor,
            now,
            pages,
        )
        .map_err(|_| BinanceExecutionError::Unavailable)?;
        self.fills_cursor = Some(candidate.fills_cursor());
        Ok((candidate, rules))
    }

    async fn snapshot(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
    ) -> Result<venue_gateway_binance::BinancePrivateReadbackCandidate, BinanceExecutionError> {
        self.snapshot_with_rules(request, credentials)
            .await
            .map(|(snapshot, _)| snapshot)
    }

    async fn exact_order(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
    ) -> Result<
        (
            venue_gateway_binance::BinancePrivateReadbackCandidate,
            venue_domain::domain::Order,
        ),
        BinanceExecutionError,
    > {
        let (snapshot, rules) = self.snapshot_with_rules(request, credentials).await?;
        let order = self
            .exact_order_for_client_in_scope(
                request,
                credentials,
                &request.client_order_id,
                snapshot.scope(),
            )
            .await?;
        let native_matches = request
            .known_native_order_id
            .as_ref()
            .is_none_or(|value| value == &order.order_id);
        (native_matches && exact_place_matches(request, &order, &rules)?)
            .then_some((snapshot, order))
            .ok_or(BinanceExecutionError::Unavailable)
    }

    async fn exact_order_for_client_in_scope(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
        client_order_id: &str,
        scope: &BinancePrivateReadScope,
    ) -> Result<venue_domain::domain::Order, BinanceExecutionError> {
        let exact = build_exact_order_request(scope, client_order_id)
            .map_err(|_| BinanceExecutionError::Invalid)?;
        let page = self
            .transport
            .execute_read(credentials, &exact, now_ms()?)
            .await
            .map_err(|_| BinanceExecutionError::Unavailable)?;
        let payload =
            std::str::from_utf8(&page.payload).map_err(|_| BinanceExecutionError::Unavailable)?;
        let order = parse_order(payload, &request.symbol)
            .map_err(|_| BinanceExecutionError::Unavailable)?;
        matches!(&order.client_order_id, FieldState::Known(value) if value == client_order_id)
            .then_some(order)
            .ok_or(BinanceExecutionError::Unavailable)
    }
}

pub type BinanceExecutionFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ExecutionOutcome, BinanceExecutionError>> + Send + 'a>>;
pub type BinanceGridBatchFuture<'a> = Pin<
    Box<dyn Future<Output = Result<GridBatchExecutionOutcome, GridBatchSubmitError>> + Send + 'a>,
>;

pub trait BinanceExecution {
    fn submit<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a>;
    fn readback<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a>;
    fn submit_grid_batch<'a>(
        &'a mut self,
        context: &'a GridBatchExecutionContext,
        requests: &'a [ExecutionRequest],
        _credentials: BinanceCredentials,
    ) -> BinanceGridBatchFuture<'a>;
}

#[allow(
    async_fn_in_trait,
    reason = "the singleton owns this narrow adapter boundary and requires no external implementation contract"
)]
pub trait BinanceActivationBaseline {
    async fn activation_baseline(
        &mut self,
        trading_account_id: &str,
        symbols: &std::collections::BTreeSet<Symbol>,
        credentials: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError>;
}

impl BinanceHttpExecution {
    async fn submit_request(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        if terminal_open::is_terminal_open(request) {
            return self.submit_terminal_open(request, &credentials).await;
        }
        let (before, rules) = self.snapshot_with_rules(request, &credentials).await?;
        if let ExecutionOrderKind::CancelExact {
            native_order_id,
            target_client_order_id,
        } = &request.order_kind
        {
            return self
                .submit_cancel(
                    request,
                    native_order_id.as_deref(),
                    target_client_order_id.as_deref(),
                    &rules,
                    &before,
                    credentials,
                )
                .await;
        }
        let (side, position_side, requested_quantity, reducing) = place_shape(request)?;
        let before_position = position_quantity(&before, position_side)?;
        let requested_quantity = if reducing {
            let reserved = reserved_close_quantity(&before, request, position_side, side)?;
            let available = before_position
                .checked_sub(reserved)
                .ok_or(BinanceExecutionError::Invalid)?;
            requested_quantity.min(available.max(Decimal::ZERO))
        } else {
            requested_quantity
        };
        let quantity = normalize_quantity(requested_quantity, &rules)?;
        if opening_minimum_notional_required(reducing) {
            check_minimum_notional(&before, position_side, quantity, &rules)?;
        }
        let prepared = match &request.order_kind {
            ExecutionOrderKind::Market { .. } => prepare_place_market(
                &rules,
                &before,
                &BinanceMarketIntent {
                    client_order_id: request.client_order_id.clone(),
                    side,
                    position_side,
                    quantity,
                    reduce_only: reducing,
                },
            ),
            ExecutionOrderKind::LimitPostOnly { price, .. } => prepare_place_limit(
                &rules,
                &before,
                &BinancePlaceIntent {
                    client_order_id: request.client_order_id.clone(),
                    side,
                    position_side,
                    quantity,
                    limit_price: venue_domain::domain::Price::new(*price)
                        .map_err(|_| BinanceExecutionError::Invalid)?,
                    time_in_force: BinanceTimeInForce::PostOnly,
                    reduce_only: reducing,
                },
            ),
            ExecutionOrderKind::CancelExact { .. } => {
                return Err(BinanceExecutionError::Invalid);
            }
        }
        .map_err(|_| BinanceExecutionError::Invalid)?;
        match self
            .transport
            .dispatch_then_exact_readback(&credentials, before.scope(), &prepared, now_ms()?)
            .await
        {
            BinancePhysicalMutationOutcome::DispatchUnknown { error } => {
                Ok(dispatch_unknown(error, None))
            }
            BinancePhysicalMutationOutcome::DispatchFailed { error } => {
                dispatch_failed(error, None)
            }
            BinancePhysicalMutationOutcome::AckedReadbackUnknown { ack, .. } => {
                Ok(outcome(ExecutionReadback::Unknown, Some(ack.order_id)))
            }
            BinancePhysicalMutationOutcome::ReadBack { ack, readback } => {
                let native_order_id = readback.order.order_id.clone();
                if native_order_id != ack.order_id {
                    return Ok(outcome(ExecutionReadback::Unknown, Some(ack.order_id)));
                }
                if exact_place_matches(request, &readback.order, &rules) != Ok(true) {
                    return Ok(outcome(ExecutionReadback::Unknown, Some(ack.order_id)));
                }
                match place_readback_decision(readback.order.state, readback.order.filled_quantity)
                {
                    PlaceReadbackDecision::Unknown => {
                        Ok(outcome(ExecutionReadback::Unknown, Some(native_order_id)))
                    }
                    PlaceReadbackDecision::Rejected => {
                        Ok(outcome(ExecutionReadback::Rejected, Some(native_order_id)))
                    }
                    PlaceReadbackDecision::Accepted => {
                        Ok(outcome(ExecutionReadback::Accepted, Some(native_order_id)))
                    }
                    PlaceReadbackDecision::VerifyTerminal => {
                        let executed = readback.order.filled_quantity;
                        if executed <= Decimal::ZERO || executed > quantity {
                            return Ok(outcome(ExecutionReadback::Unknown, Some(native_order_id)));
                        }
                        let after = match self.snapshot(request, &credentials).await {
                            Ok(after) => after,
                            Err(_) => {
                                return Ok(outcome(
                                    ExecutionReadback::Unknown,
                                    Some(native_order_id),
                                ));
                            }
                        };
                        Ok(outcome(
                            converged(
                                position_side,
                                reducing,
                                executed,
                                before_position,
                                &after,
                                &readback.order,
                            ),
                            Some(native_order_id),
                        ))
                    }
                }
            }
        }
    }

    async fn submit_cancel(
        &mut self,
        _request: &ExecutionRequest,
        selected_native_order_id: Option<&str>,
        selected_client_order_id: Option<&str>,
        rules: &venue_gateway_binance::BinanceInstrumentRules,
        before: &venue_gateway_binance::BinancePrivateReadbackCandidate,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        let Some((native_order_id, target_client_order_id)) =
            cancel_target(before, selected_native_order_id, selected_client_order_id)?
        else {
            // `before` is a complete signed ordinary-order surface. Once neither exact selector
            // is present, the cancel objective is already true and no mutation may be sent.
            return Ok(outcome(
                ExecutionReadback::Reconciled,
                selected_native_order_id.map(str::to_owned),
            ));
        };
        let prepared = prepare_cancel(
            rules,
            before,
            &BinanceCancelIntent {
                client_order_id: target_client_order_id,
            },
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        match self
            .transport
            .dispatch_then_exact_readback(&credentials, before.scope(), &prepared, now_ms()?)
            .await
        {
            BinancePhysicalMutationOutcome::DispatchUnknown { error } => {
                Ok(dispatch_unknown(error, Some(native_order_id.clone())))
            }
            BinancePhysicalMutationOutcome::DispatchFailed { error } => {
                dispatch_failed(error, Some(native_order_id.clone()))
            }
            BinancePhysicalMutationOutcome::AckedReadbackUnknown { ack, .. } => {
                Ok(outcome(ExecutionReadback::Unknown, Some(ack.order_id)))
            }
            BinancePhysicalMutationOutcome::ReadBack { ack, readback } => {
                if ack.order_id != native_order_id || readback.order.order_id != native_order_id {
                    return Ok(outcome(
                        ExecutionReadback::Unknown,
                        Some(native_order_id.clone()),
                    ));
                }
                let state = if terminal_order_state(readback.order.state) {
                    ExecutionReadback::Reconciled
                } else {
                    ExecutionReadback::Accepted
                };
                Ok(outcome(state, Some(native_order_id)))
            }
        }
    }

    async fn readback_cancel(
        &mut self,
        request: &ExecutionRequest,
        selected_native_order_id: Option<&str>,
        selected_client_order_id: Option<&str>,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        let snapshot = self.snapshot(request, &credentials).await?;
        if let Some((native_order_id, _)) = cancel_target(
            &snapshot,
            selected_native_order_id,
            selected_client_order_id,
        )? {
            return Ok(outcome(ExecutionReadback::Accepted, Some(native_order_id)));
        }
        // The complete signed open-order surface is the cancellation postcondition. Exact order
        // history is useful evidence but not required and may legitimately return not-found.
        Ok(outcome(
            ExecutionReadback::Reconciled,
            selected_native_order_id
                .map(str::to_owned)
                .or_else(|| request.known_native_order_id.clone()),
        ))
    }

    async fn readback_request(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        if let ExecutionOrderKind::CancelExact {
            native_order_id,
            target_client_order_id,
        } = &request.order_kind
        {
            return self
                .readback_cancel(
                    request,
                    native_order_id.as_deref(),
                    target_client_order_id.as_deref(),
                    credentials,
                )
                .await;
        }
        let (after, order) = match self.exact_order(request, &credentials).await {
            Ok(readback) => readback,
            // A missing exact order is never proof that Binance did not receive the POST.
            Err(BinanceExecutionError::Unavailable) => {
                return Ok(outcome(
                    ExecutionReadback::Unknown,
                    request.known_native_order_id.clone(),
                ));
            }
            Err(error) => return Err(error),
        };
        let native_order_id = order.order_id.clone();
        let state = match place_readback_decision(order.state, order.filled_quantity) {
            PlaceReadbackDecision::Unknown => ExecutionReadback::Unknown,
            PlaceReadbackDecision::Rejected => ExecutionReadback::Rejected,
            PlaceReadbackDecision::Accepted => ExecutionReadback::Accepted,
            PlaceReadbackDecision::VerifyTerminal => restart_converged(request, &after, &order),
        };
        Ok(outcome(state, Some(native_order_id)))
    }
}

impl BinanceExecution for BinanceHttpExecution {
    fn submit<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(self.submit_request(request, credentials))
    }

    fn readback<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(self.readback_request(request, credentials))
    }

    fn submit_grid_batch<'a>(
        &'a mut self,
        _context: &'a GridBatchExecutionContext,
        requests: &'a [ExecutionRequest],
        credentials: BinanceCredentials,
    ) -> BinanceGridBatchFuture<'a> {
        Box::pin(async move {
            // The helper completes every fallible validation/signing step before starting its
            // dispatch burst; once the burst starts it records only per-command outcomes.
            self.submit_grid_batch_request(requests, credentials)
                .await
                .map_err(GridBatchSubmitError::DefinitelyNotDispatched)
        })
    }
}

impl BinanceExecution for BinanceExecutionRouter {
    fn submit<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        let exchange = self.exchange(request);
        Box::pin(async move {
            let exchange = exchange?;
            exchange.lock().await.submit(request, credentials).await
        })
    }

    fn readback<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        let exchange = self.exchange(request);
        Box::pin(async move {
            let exchange = exchange?;
            exchange.lock().await.readback(request, credentials).await
        })
    }

    fn submit_grid_batch<'a>(
        &'a mut self,
        context: &'a GridBatchExecutionContext,
        requests: &'a [ExecutionRequest],
        credentials: BinanceCredentials,
    ) -> BinanceGridBatchFuture<'a> {
        let executor_started = Instant::now();
        let hot_token = self.take_matching_hot_token(context, requests);
        let exchange = requests
            .first()
            .ok_or(BinanceExecutionError::Invalid)
            .and_then(|request| self.exchange(request));
        Box::pin(async move {
            let exchange = exchange.map_err(GridBatchSubmitError::DefinitelyNotDispatched)?;
            let mut exchange = exchange.lock().await;
            if let Some(durable) = &context.durable {
                exchange
                    .transport
                    .rebind_generations(durable.instrument_generation, durable.private_generation)
                    .map_err(|_| {
                        GridBatchSubmitError::DefinitelyNotDispatched(
                            BinanceExecutionError::Unavailable,
                        )
                    })?;
            }
            if let Some(token) = hot_token {
                match exchange
                    .submit_grid_batch_hot_request(requests, &credentials, &token, executor_started)
                    .await
                {
                    Ok(outcome) => {
                        tracing::info!(target: "venue_control::grid_hot_path",
                            batch_id = %context.batch_id,
                            "Grid batch dispatched using authenticated hot facts without REST preflight");
                        return Ok(outcome);
                    }
                    Err(error) => tracing::warn!(
                        target: "venue_control::grid_hot_path",
                        batch_id = %context.batch_id,
                        error_code = error.not_dispatched_code(),
                        "Grid hot dispatch fell back before any physical send"
                    ),
                }
            }
            exchange
                .submit_grid_batch_request_started(requests, credentials, executor_started)
                .await
                .map_err(GridBatchSubmitError::DefinitelyNotDispatched)
        })
    }
}

impl BinanceActivationBaseline for BinanceHttpExecution {
    async fn activation_baseline(
        &mut self,
        trading_account_id: &str,
        symbols: &std::collections::BTreeSet<Symbol>,
        _credentials: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError> {
        // Activation scans the whole account and all configured symbols. This per-symbol order
        // adapter must not pretend that one position-risk response proves that wider boundary.
        if symbols.is_empty()
            || trading_account_id != self.transport.config().gateway_binding().trading_account_id
        {
            return Err(BinanceExecutionError::Invalid);
        }
        Err(BinanceExecutionError::Unavailable)
    }
}

impl BinanceActivationBaseline for BinanceExecutionRouter {
    async fn activation_baseline(
        &mut self,
        trading_account_id: &str,
        symbols: &std::collections::BTreeSet<Symbol>,
        credentials: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError> {
        let primary_symbol = symbols
            .first()
            .cloned()
            .ok_or(BinanceExecutionError::Invalid)?;
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            trading_account_id.to_owned(),
            primary_symbol,
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        let symbols = symbols.clone();
        let limits = self.limits;
        tokio::task::spawn_blocking(move || {
            BinanceAccountGateway::connect_with_credentials_for_symbols(
                binding,
                symbols,
                credentials,
                limits,
            )
            .map(|_| AccountBaseline::Clean)
            .map_err(|_| BinanceExecutionError::Unavailable)
        })
        .await
        .map_err(|_| BinanceExecutionError::Unavailable)?
    }
}

fn validate_request_binding(
    transport: &BinanceHttpTransport,
    request: &ExecutionRequest,
) -> Result<(), BinanceExecutionError> {
    let binding = transport.config().gateway_binding();
    if request.command_id.is_empty()
        || request.client_order_id.is_empty()
        || request.credential_id.is_empty()
        || request.trading_account_id != binding.trading_account_id
        || request.symbol != binding.symbol
        || request
            .known_native_order_id
            .as_deref()
            .is_some_and(invalid_native_order_id)
    {
        return Err(BinanceExecutionError::Invalid);
    }
    match &request.order_kind {
        ExecutionOrderKind::Market {
            side,
            position_side,
            quantity,
            reducing,
        }
        | ExecutionOrderKind::LimitPostOnly {
            side,
            position_side,
            quantity,
            reducing,
            ..
        } => {
            let price_invalid = matches!(
                &request.order_kind,
                ExecutionOrderKind::LimitPostOnly { price, .. } if *price <= Decimal::ZERO
            );
            if *position_side == PositionSide::Net
                || *quantity <= Decimal::ZERO
                || price_invalid
                || (!*reducing && !request.reconciled_close_reservations.is_empty())
                || *reducing
                    != matches!(
                        (*position_side, *side),
                        (PositionSide::Long, OrderSide::Sell)
                            | (PositionSide::Short, OrderSide::Buy)
                    )
            {
                return Err(BinanceExecutionError::Invalid);
            }
        }
        ExecutionOrderKind::CancelExact {
            native_order_id,
            target_client_order_id,
        } => {
            if native_order_id.is_none() && target_client_order_id.is_none()
                || !request.reconciled_close_reservations.is_empty()
                || native_order_id
                    .as_deref()
                    .is_some_and(invalid_native_order_id)
                || target_client_order_id
                    .as_deref()
                    .is_some_and(invalid_native_order_id)
                || native_order_id.as_ref().is_some_and(|selected| {
                    request
                        .known_native_order_id
                        .as_ref()
                        .is_some_and(|known| known != selected)
                })
            {
                return Err(BinanceExecutionError::Invalid);
            }
        }
    }
    Ok(())
}

fn invalid_native_order_id(value: &str) -> bool {
    value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_whitespace)
}

fn cancel_target(
    snapshot: &venue_gateway_binance::BinancePrivateReadbackCandidate,
    selected_native_order_id: Option<&str>,
    selected_client_order_id: Option<&str>,
) -> Result<Option<(String, String)>, BinanceExecutionError> {
    if selected_native_order_id.is_none() && selected_client_order_id.is_none() {
        return Err(BinanceExecutionError::Invalid);
    }
    let mut selected = None;
    for order in &snapshot.regular().orders {
        let client_order_id = match &order.client_order_id {
            FieldState::Known(value) => Some(value),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => None,
        };
        let native_selected = selected_native_order_id == Some(order.order_id.as_str());
        let client_selected = selected_client_order_id
            .is_some_and(|expected| client_order_id.is_some_and(|actual| actual == expected));
        let matches = cancel_selectors_match(
            order.order_id.as_str(),
            client_order_id.map(String::as_str),
            selected_native_order_id,
            selected_client_order_id,
        );
        if (native_selected || client_selected) && !matches {
            // Dual selectors that point at different live orders are a binding conflict, not
            // evidence that the intended order has already disappeared.
            return Err(BinanceExecutionError::Unavailable);
        }
        if !matches {
            continue;
        }
        let client_order_id = client_order_id.ok_or(BinanceExecutionError::Unavailable)?;
        if selected.is_some() {
            return Err(BinanceExecutionError::Unavailable);
        }
        selected = Some((order.order_id.clone(), client_order_id.clone()));
    }
    Ok(selected)
}

fn cancel_selectors_match(
    order_native_id: &str,
    order_client_id: Option<&str>,
    selected_native_order_id: Option<&str>,
    selected_client_order_id: Option<&str>,
) -> bool {
    selected_native_order_id.is_none_or(|expected| expected == order_native_id)
        && selected_client_order_id
            .is_none_or(|expected| order_client_id.is_some_and(|actual| actual == expected))
}

fn place_shape(
    request: &ExecutionRequest,
) -> Result<(OrderSide, PositionSide, Decimal, bool), BinanceExecutionError> {
    match &request.order_kind {
        ExecutionOrderKind::Market {
            side,
            position_side,
            quantity,
            reducing,
        }
        | ExecutionOrderKind::LimitPostOnly {
            side,
            position_side,
            quantity,
            reducing,
            ..
        } => Ok((*side, *position_side, *quantity, *reducing)),
        ExecutionOrderKind::CancelExact { .. } => Err(BinanceExecutionError::Invalid),
    }
}

/// An exact client ID is necessary but not sufficient evidence that Binance observed this
/// durable mutation. Every immutable order field is checked after exchange-rule normalization;
/// Portfolio Margin hedge closes intentionally do not compare native `reduceOnly`, which Binance
/// forbids on those orders and which is instead encoded by side plus position side.
fn exact_place_matches(
    request: &ExecutionRequest,
    order: &venue_domain::domain::Order,
    rules: &venue_gateway_binance::BinanceInstrumentRules,
) -> Result<bool, BinanceExecutionError> {
    let (side, position_side, requested_quantity, reducing) = place_shape(request)?;
    let quantity = normalize_quantity(requested_quantity, rules)?;
    let quantity_matches = if reducing {
        order.quantity > Decimal::ZERO
            && order.quantity <= quantity
            && normalize_quantity(order.quantity, rules)? == order.quantity
    } else {
        order.quantity == quantity
    };
    let common = order.client_order_id == FieldState::Known(request.client_order_id.clone())
        && order.symbol == request.symbol
        && order.side == side
        && order.position_side == FieldState::Known(position_side)
        && quantity_matches;
    if !common {
        return Ok(false);
    }
    Ok(match &request.order_kind {
        ExecutionOrderKind::Market { .. } => order.limit_price.is_none(),
        ExecutionOrderKind::LimitPostOnly { price, .. } => {
            order
                .limit_price
                .is_some_and(|value| value.value() == *price)
                && order.time_in_force == FieldState::Known(LimitTimeInForce::PostOnly)
        }
        ExecutionOrderKind::CancelExact { .. } => false,
    })
}

fn normalize_quantity(
    quantity: Decimal,
    rules: &venue_gateway_binance::BinanceInstrumentRules,
) -> Result<Decimal, BinanceExecutionError> {
    let normalized = quantity - quantity % rules.instrument.quantity_step;
    if normalized <= Decimal::ZERO
        || normalized < rules.minimum_quantity
        || normalized > rules.maximum_quantity
    {
        return Err(BinanceExecutionError::Invalid);
    }
    Ok(normalized)
}

fn check_minimum_notional(
    readback: &venue_gateway_binance::BinancePrivateReadbackCandidate,
    side: PositionSide,
    quantity: Decimal,
    rules: &venue_gateway_binance::BinanceInstrumentRules,
) -> Result<(), BinanceExecutionError> {
    let price = readback
        .positions()
        .iter()
        .find(|position| position.side == side)
        .and_then(|position| position.mark_price)
        .ok_or(BinanceExecutionError::Invalid)?;
    quantity
        .checked_mul(price.value())
        .filter(|value| *value >= rules.instrument.minimum_notional.value)
        .map(|_| ())
        .ok_or(BinanceExecutionError::Invalid)
}

const fn opening_minimum_notional_required(reducing: bool) -> bool {
    !reducing
}

fn converged(
    position_side: PositionSide,
    reducing: bool,
    quantity: Decimal,
    before_quantity: Decimal,
    after: &venue_gateway_binance::BinancePrivateReadbackCandidate,
    order: &venue_domain::domain::Order,
) -> ExecutionReadback {
    let fills = after
        .fills()
        .iter()
        .filter(|fill| fill.order_id == order.order_id)
        .try_fold(Decimal::ZERO, |total, fill| {
            total.checked_add(fill.quantity)
        });
    let after_quantity = after
        .positions()
        .iter()
        .find(|position| position.side == position_side)
        .map(|position| position.quantity);
    let expected = if reducing {
        before_quantity.checked_sub(quantity)
    } else {
        before_quantity.checked_add(quantity)
    };
    if fills == Some(quantity) && after_quantity == expected {
        ExecutionReadback::Reconciled
    } else {
        // A terminal native order is not a live accepted maker. Until accountTradeList and the
        // signed position agree on its executed quantity, the mutation remains uncertain.
        ExecutionReadback::Unknown
    }
}

fn restart_converged(
    request: &ExecutionRequest,
    after: &venue_gateway_binance::BinancePrivateReadbackCandidate,
    order: &venue_domain::domain::Order,
) -> ExecutionReadback {
    // A market mutation cannot be reconstructed from a terminal order and the current leg alone:
    // its durable command does not yet retain the signed pre-dispatch leg quantity. Without that
    // baseline a restart cannot prove that the position moved by this order's exact fill.
    if !matches!(request.order_kind, ExecutionOrderKind::LimitPostOnly { .. }) {
        return ExecutionReadback::Unknown;
    }
    let fills = after
        .fills()
        .iter()
        .filter(|fill| fill.order_id == order.order_id)
        .try_fold(Decimal::ZERO, |total, fill| {
            total.checked_add(fill.quantity)
        });
    let absent_from_signed_open_surface = after.regular().orders.iter().all(|open| {
        open.order_id != order.order_id && open.client_order_id != order.client_order_id
    });
    let signed_terminal = terminal_order_state(order.state)
        && order.state != OrderState::Rejected
        && order.filled_quantity > Decimal::ZERO
        && fills == Some(order.filled_quantity)
        && absent_from_signed_open_surface;
    restart_terminal_decision(&request.order_kind, signed_terminal)
}

fn restart_terminal_decision(
    order_kind: &ExecutionOrderKind,
    signed_terminal: bool,
) -> ExecutionReadback {
    // A market mutation cannot be reconstructed from a terminal order and the current leg alone:
    // its durable command does not yet retain the signed pre-dispatch leg quantity. Without that
    // baseline a restart cannot prove that the position moved by this order's exact fill.
    if signed_terminal && matches!(order_kind, ExecutionOrderKind::LimitPostOnly { .. }) {
        ExecutionReadback::Reconciled
    } else {
        ExecutionReadback::Unknown
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlaceReadbackDecision {
    Accepted,
    VerifyTerminal,
    Rejected,
    Unknown,
}

fn place_readback_decision(state: OrderState, filled_quantity: Decimal) -> PlaceReadbackDecision {
    match state {
        OrderState::Unknown => PlaceReadbackDecision::Unknown,
        OrderState::New | OrderState::PartiallyFilled => PlaceReadbackDecision::Accepted,
        OrderState::Filled if filled_quantity > Decimal::ZERO => {
            PlaceReadbackDecision::VerifyTerminal
        }
        OrderState::Filled => PlaceReadbackDecision::Unknown,
        OrderState::Cancelled | OrderState::Expired if filled_quantity > Decimal::ZERO => {
            PlaceReadbackDecision::VerifyTerminal
        }
        OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
            if filled_quantity == Decimal::ZERO =>
        {
            PlaceReadbackDecision::Rejected
        }
        OrderState::Rejected | OrderState::Cancelled | OrderState::Expired => {
            PlaceReadbackDecision::Unknown
        }
    }
}

fn terminal_order_state(state: OrderState) -> bool {
    matches!(
        state,
        OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
    )
}

fn outcome(state: ExecutionReadback, native_order_id: Option<String>) -> ExecutionOutcome {
    ExecutionOutcome {
        exchange_error_code: None,
        state,
        native_order_id,
    }
}

fn dispatch_unknown(
    error: BinanceTransportError,
    native_order_id: Option<String>,
) -> ExecutionOutcome {
    debug_assert!(error.is_unknown_dispatch());
    outcome(ExecutionReadback::Unknown, native_order_id)
}

fn dispatch_failed(
    error: BinanceTransportError,
    native_order_id: Option<String>,
) -> Result<ExecutionOutcome, BinanceExecutionError> {
    let result = match error {
        BinanceTransportError::ApiRejected(code) => {
            let mut rejected = outcome(ExecutionReadback::Rejected, native_order_id);
            rejected.exchange_error_code = Some(code);
            rejected
        }
        // `AmbiguousStatus` already owns timeout/5xx cases where Binance may have accepted the
        // mutation. Any `HttpStatus` here is a complete explicit rejection response.
        BinanceTransportError::HttpStatus(_) => {
            outcome(ExecutionReadback::Rejected, native_order_id)
        }
        BinanceTransportError::TimestampRejected => {
            let mut rejected = outcome(ExecutionReadback::Rejected, native_order_id);
            rejected.exchange_error_code = Some(-1021);
            rejected
        }
        // These checks occur while binding or constructing the signed request, before reqwest's
        // physical send future is entered.
        BinanceTransportError::Binding => return Err(BinanceExecutionError::Invalid),
        BinanceTransportError::Limits
        | BinanceTransportError::Signing
        | BinanceTransportError::Http
        | BinanceTransportError::Payload
        | BinanceTransportError::Protocol
        | BinanceTransportError::EndOfStream => {
            return Err(BinanceExecutionError::Unavailable);
        }
        // A bounded body/ACK failure is after POST. Clock can also fail while timestamping the
        // received response, so it cannot safely be downgraded to NotDispatched.
        BinanceTransportError::Timeout
        | BinanceTransportError::Disconnected
        | BinanceTransportError::AmbiguousStatus(_)
        | BinanceTransportError::BodyTooLarge
        | BinanceTransportError::Ack
        | BinanceTransportError::Clock => outcome(ExecutionReadback::Unknown, native_order_id),
    };
    Ok(result)
}

fn position_quantity(
    readback: &venue_gateway_binance::BinancePrivateReadbackCandidate,
    side: PositionSide,
) -> Result<Decimal, BinanceExecutionError> {
    readback
        .positions()
        .iter()
        .find(|position| position.side == side)
        .map(|position| position.quantity)
        .ok_or(BinanceExecutionError::Unavailable)
}

fn reserved_close_quantity(
    readback: &venue_gateway_binance::BinancePrivateReadbackCandidate,
    request: &ExecutionRequest,
    position_side: PositionSide,
    close_side: OrderSide,
) -> Result<Decimal, BinanceExecutionError> {
    let mut total = readback
        .regular()
        .orders
        .iter()
        .chain(readback.algo().orders.iter())
        .filter(|order| {
            order.symbol == request.symbol
                && order.side == close_side
                && order.position_side == FieldState::Known(position_side)
                && !terminal_order_state(order.state)
        })
        .try_fold(Decimal::ZERO, |total, order| {
            let remaining = order
                .quantity
                .checked_sub(order.filled_quantity)
                .filter(|value| *value >= Decimal::ZERO)
                .ok_or(BinanceExecutionError::Invalid)?;
            total
                .checked_add(remaining)
                .ok_or(BinanceExecutionError::Invalid)
        })?;
    let mut seen = BTreeSet::new();
    for reservation in &request.reconciled_close_reservations {
        if !seen.insert(reservation.client_order_id.as_str()) {
            return Err(BinanceExecutionError::Invalid);
        }
        if !reservation_applies(request, reservation, position_side, close_side)? {
            continue;
        }
        let visible = readback
            .regular()
            .orders
            .iter()
            .chain(readback.algo().orders.iter())
            .find(|order| {
                matches!(
                    &order.client_order_id,
                    FieldState::Known(value) if value == &reservation.client_order_id
                )
            });
        if let Some(order) = visible {
            if order.symbol != request.symbol
                || order.side != close_side
                || order.position_side != FieldState::Known(position_side)
                || terminal_order_state(order.state)
            {
                return Err(BinanceExecutionError::Unavailable);
            }
            // The signed ordinary/Algo sum above already reserves this exact order.
            continue;
        }
        total = total
            .checked_add(reservation.quantity)
            .ok_or(BinanceExecutionError::Invalid)?;
    }
    Ok(total)
}

fn reservation_applies(
    request: &ExecutionRequest,
    reservation: &ReconciledCloseReservation,
    position_side: PositionSide,
    close_side: OrderSide,
) -> Result<bool, BinanceExecutionError> {
    if reservation.credential_id != request.credential_id
        || reservation.trading_account_id != request.trading_account_id
        || reservation.symbol != request.symbol
        || reservation.client_order_id.is_empty()
        || reservation.quantity <= Decimal::ZERO
        || reservation.projection_observed_ms == 0
        || reservation.reconciled_ms <= reservation.projection_observed_ms
    {
        return Err(BinanceExecutionError::Invalid);
    }
    Ok(reservation.side == close_side && reservation.position_side == position_side)
}

fn now_ms() -> Result<u64, BinanceExecutionError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceExecutionError::Unavailable)?
        .as_millis();
    u64::try_from(value).map_err(|_| BinanceExecutionError::Unavailable)
}

#[derive(Clone, Default)]
pub struct MockBinanceExecution {
    orders: BTreeMap<String, ExecutionOutcome>,
    baselines: BTreeMap<String, AccountBaseline>,
    grid_batch_failure: Option<GridBatchSubmitError>,
    grid_batch_dispatch_started: Arc<AtomicBool>,
}

impl MockBinanceExecution {
    pub fn set_rejection(&mut self, client_order_id: String, code: i64) {
        let mut result = outcome(ExecutionReadback::Rejected, None);
        result.exchange_error_code = Some(code);
        self.orders.insert(client_order_id, result);
    }

    pub fn set_readback(&mut self, client_order_id: String, state: ExecutionReadback) {
        let native_order_id = matches!(
            state,
            ExecutionReadback::Accepted | ExecutionReadback::Reconciled
        )
        .then(|| format!("mock-{client_order_id}"));
        self.orders
            .insert(client_order_id, outcome(state, native_order_id));
    }

    pub fn set_baseline(&mut self, trading_account_id: String, baseline: AccountBaseline) {
        self.baselines.insert(trading_account_id, baseline);
    }

    pub fn set_grid_batch_failure(&mut self, failure: GridBatchSubmitError) {
        self.grid_batch_failure = Some(failure);
    }

    #[must_use]
    pub fn grid_batch_dispatch_started(&self) -> bool {
        self.grid_batch_dispatch_started.load(Ordering::Acquire)
    }
}

impl BinanceExecution for MockBinanceExecution {
    fn submit<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        _credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(async move {
            if request.client_order_id.is_empty()
                || request.command_id.is_empty()
                || request.trading_account_id.is_empty()
            {
                return Err(BinanceExecutionError::Invalid);
            }
            let default_native = match &request.order_kind {
                ExecutionOrderKind::CancelExact {
                    native_order_id, ..
                } => native_order_id
                    .clone()
                    .or_else(|| request.known_native_order_id.clone())
                    .unwrap_or_else(|| format!("mock-target-{}", request.client_order_id)),
                ExecutionOrderKind::Market { .. } | ExecutionOrderKind::LimitPostOnly { .. } => {
                    format!("mock-{}", request.client_order_id)
                }
            };
            Ok(self
                .orders
                .entry(request.client_order_id.clone())
                .or_insert_with(|| outcome(ExecutionReadback::Accepted, Some(default_native)))
                .clone())
        })
    }

    fn readback<'a>(
        &'a mut self,
        request: &'a ExecutionRequest,
        _credentials: BinanceCredentials,
    ) -> BinanceExecutionFuture<'a> {
        Box::pin(async move {
            self.orders
                .get(&request.client_order_id)
                .cloned()
                .ok_or(BinanceExecutionError::Unavailable)
        })
    }

    fn submit_grid_batch<'a>(
        &'a mut self,
        _context: &'a GridBatchExecutionContext,
        requests: &'a [ExecutionRequest],
        _credentials: BinanceCredentials,
    ) -> BinanceGridBatchFuture<'a> {
        Box::pin(async move {
            validate_grid_batch_shape(requests)
                .map_err(GridBatchSubmitError::DefinitelyNotDispatched)?;
            if let Some(failure) = self.grid_batch_failure.take() {
                if failure == GridBatchSubmitError::DispatchUncertain {
                    self.grid_batch_dispatch_started
                        .store(true, Ordering::Release);
                }
                return Err(failure);
            }
            if requests.iter().any(|request| {
                request.command_id.is_empty()
                    || request.client_order_id.is_empty()
                    || request.trading_account_id.is_empty()
            }) {
                return Err(GridBatchSubmitError::DefinitelyNotDispatched(
                    BinanceExecutionError::Invalid,
                ));
            }
            self.grid_batch_dispatch_started
                .store(true, Ordering::Release);
            let started = Instant::now();
            let mut commands = Vec::with_capacity(requests.len());
            let mut first = None;
            let mut last = None;
            let mut attempts = 0_u16;
            for request in requests {
                let submit_us = elapsed_us(started);
                let native = match &request.order_kind {
                    ExecutionOrderKind::CancelExact {
                        native_order_id, ..
                    } => native_order_id
                        .clone()
                        .or_else(|| request.known_native_order_id.clone())
                        .unwrap_or_else(|| format!("mock-target-{}", request.client_order_id)),
                    ExecutionOrderKind::Market { .. }
                    | ExecutionOrderKind::LimitPostOnly { .. } => {
                        format!("mock-{}", request.client_order_id)
                    }
                };
                let result = self
                    .orders
                    .entry(request.client_order_id.clone())
                    .or_insert_with(|| outcome(ExecutionReadback::Accepted, Some(native)))
                    .clone();
                record_outbound_timing(submit_us, &mut first, &mut last, &mut attempts);
                commands.push(GridBatchCommandOutcome::Submitted(result));
            }
            Ok(GridBatchExecutionOutcome {
                commands,
                timing: GridBatchSubmitTiming {
                    executor_start_to_first_submit_us: first,
                    executor_start_to_last_submit_us: last,
                    first_to_last_submit_us: first
                        .zip(last)
                        .map(|(first, last)| last.saturating_sub(first)),
                    outbound_attempts: attempts,
                },
            })
        })
    }
}

impl BinanceActivationBaseline for MockBinanceExecution {
    async fn activation_baseline(
        &mut self,
        trading_account_id: &str,
        _symbols: &std::collections::BTreeSet<Symbol>,
        _credentials: BinanceCredentials,
    ) -> Result<AccountBaseline, BinanceExecutionError> {
        if trading_account_id.is_empty() {
            return Err(BinanceExecutionError::Invalid);
        }
        Ok(self
            .baselines
            .get(trading_account_id)
            .copied()
            .unwrap_or(AccountBaseline::Clean))
    }
}

#[cfg(test)]
#[path = "executor_exchange/tests.rs"]
mod tests;
