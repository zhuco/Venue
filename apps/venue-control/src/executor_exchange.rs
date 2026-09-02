//! Testable signed-execution boundary. Production wiring will adapt this trait to the existing
//! Binance transport; mocks exercise idempotency and readback without a network path.

use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use venue_domain::domain::{FieldState, OrderSide, OrderState, PositionSide, Symbol};
use venue_gateway_binance::{BinanceAccountGateway, BinanceCredentials};
use venue_gateway_binance::{
    BinanceHttpTransport, BinanceMarketIntent, BinancePhysicalMutationOutcome,
    BinancePrivateReadScope, build_account_config_request, build_account_request,
    build_algo_orders_request, build_exact_order_request, build_fills_request,
    build_position_mode_request, build_positions_request, build_regular_orders_request,
    complete_private_readback, parse_instrument_rules, prepare_place_market,
    private::{RecentFillsCursor, parse_order},
};
use venue_gateway_binance::{GatewayBinding, GatewayMode, VenueId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionRequest {
    pub command_id: String,
    pub client_order_id: String,
    pub trading_account_id: String,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub quantity: Decimal,
    /// A close is only a domain-level reduction. The Binance Hedge request deliberately omits
    /// native reduceOnly and relies on side, leg, clipped quantity, and signed convergence.
    pub reducing: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionReadback {
    Accepted,
    Reconciled,
    Rejected,
    Unknown,
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
}

/// The production Portfolio Margin UM adapter. It owns no credentials and never retries a
/// mutation: a durable command ID can pass through `submit` once, while later calls only use
/// `readback`. The supplied transport is the existing signed HTTP implementation, so fixtures
/// can exercise the exact same request/signature surface without a second client.
pub struct BinanceHttpExecution {
    transport: BinanceHttpTransport,
    fills_cursor: Option<RecentFillsCursor>,
    pre_dispatch_positions: BTreeMap<String, Decimal>,
    next_attempt_id: u64,
}

/// Routes one singleton's durable commands to an account-and-symbol-bound HTTP adapter. The
/// map only retains transport connection state; PostgreSQL remains the command authority.
pub struct BinanceExecutionRouter {
    exchanges: BTreeMap<(String, Symbol), BinanceHttpExecution>,
    limits: venue_gateway_binance::BinanceTransportLimits,
}

impl BinanceExecutionRouter {
    #[must_use]
    pub fn new(limits: venue_gateway_binance::BinanceTransportLimits) -> Self {
        Self {
            exchanges: BTreeMap::new(),
            limits,
        }
    }

    fn exchange(
        &mut self,
        request: &ExecutionRequest,
    ) -> Result<&mut BinanceHttpExecution, BinanceExecutionError> {
        let key = (request.trading_account_id.clone(), request.symbol.clone());
        if !self.exchanges.contains_key(&key) {
            let binding = GatewayBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                request.trading_account_id.clone(),
                request.symbol.clone(),
            )
            .map_err(|_| BinanceExecutionError::Invalid)?;
            let config = venue_gateway_binance::BinanceConfig::for_binding(
                venue_gateway_binance::BinanceAccountBinding::PortfolioMarginUm,
                &binding,
            )
            .map_err(|_| BinanceExecutionError::Invalid)?;
            let transport = BinanceHttpTransport::new(config, 1, 1, self.limits)
                .map_err(|_| BinanceExecutionError::Unavailable)?;
            self.exchanges
                .insert(key.clone(), BinanceHttpExecution::new(transport));
        }
        self.exchanges
            .get_mut(&key)
            .ok_or(BinanceExecutionError::Unavailable)
    }
}

impl BinanceHttpExecution {
    #[must_use]
    pub fn new(transport: BinanceHttpTransport) -> Self {
        Self {
            transport,
            fills_cursor: None,
            pre_dispatch_positions: BTreeMap::new(),
            next_attempt_id: 1,
        }
    }

    async fn snapshot(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
    ) -> Result<venue_gateway_binance::BinancePrivateReadbackCandidate, BinanceExecutionError> {
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
            // A fresh executor has no persisted fill cursor in this narrow order adapter. Start
            // from the fixed Binance window and fail closed if the first page cannot prove it is
            // complete; a later private-stream integration owns durable cursor recovery.
            observed_through_ms: now.saturating_sub(7 * 24 * 60 * 60 * 1_000),
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
            build_positions_request(&scope),
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
        Ok(candidate)
    }

    async fn exact_order(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
    ) -> Result<venue_domain::domain::Order, BinanceExecutionError> {
        let snapshot = self.snapshot(request, credentials).await?;
        let exact = build_exact_order_request(snapshot.scope(), &request.client_order_id)
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
        matches!(&order.client_order_id, FieldState::Known(value) if value == &request.client_order_id)
            .then_some(order)
            .ok_or(BinanceExecutionError::Unavailable)
    }
}

#[allow(
    async_fn_in_trait,
    reason = "executor owns the only implementation boundary and requires no downstream trait contract"
)]
pub trait BinanceExecution {
    async fn submit(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError>;
    async fn readback(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError>;
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

impl BinanceExecution for BinanceHttpExecution {
    async fn submit(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError> {
        let before = self.snapshot(request, &credentials).await?;
        let rules = rules_for_request(&self.transport, request, &before).await?;
        let quantity = normalize_quantity(request.quantity, &rules)?;
        check_minimum_notional(&before, request.position_side, quantity, &rules)?;
        let before_position = position_quantity(&before, request.position_side)?;
        self.pre_dispatch_positions
            .insert(request.client_order_id.clone(), before_position);
        let prepared = prepare_place_market(
            &rules,
            &before,
            &BinanceMarketIntent {
                client_order_id: request.client_order_id.clone(),
                side: request.side,
                position_side: request.position_side,
                quantity,
                reduce_only: request.reducing,
            },
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        match self
            .transport
            .dispatch_then_exact_readback(&credentials, before.scope(), &prepared, now_ms()?)
            .await
        {
            BinancePhysicalMutationOutcome::DispatchUnknown { .. } => {
                Ok(ExecutionReadback::Unknown)
            }
            BinancePhysicalMutationOutcome::DispatchFailed {
                error: venue_gateway_binance::BinanceTransportError::HttpStatus(_),
            } => Ok(ExecutionReadback::Rejected),
            // A malformed ACK is not a rejection. The physical POST may have reached Binance,
            // so preserve the readback-only fence just as for a timeout or disconnect.
            BinancePhysicalMutationOutcome::DispatchFailed { .. } => Ok(ExecutionReadback::Unknown),
            BinancePhysicalMutationOutcome::AckedReadbackUnknown { .. } => {
                Ok(ExecutionReadback::Accepted)
            }
            BinancePhysicalMutationOutcome::ReadBack { readback, .. } => {
                if readback.order.state == OrderState::Rejected {
                    self.pre_dispatch_positions.remove(&request.client_order_id);
                    return Ok(ExecutionReadback::Rejected);
                }
                if readback.order.state != OrderState::Filled
                    || readback.order.filled_quantity != quantity
                {
                    return Ok(ExecutionReadback::Accepted);
                }
                let after = self.snapshot(request, &credentials).await?;
                let result = converged(request, quantity, before_position, &after, &readback.order);
                if result == ExecutionReadback::Reconciled {
                    self.pre_dispatch_positions.remove(&request.client_order_id);
                }
                Ok(result)
            }
        }
    }

    async fn readback(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError> {
        let order = match self.exact_order(request, &credentials).await {
            Ok(order) => order,
            // A missing exact order is never proof that Binance did not receive the POST.
            Err(BinanceExecutionError::Unavailable) => return Ok(ExecutionReadback::Unknown),
            Err(error) => return Err(error),
        };
        if order.state == OrderState::Rejected {
            self.pre_dispatch_positions.remove(&request.client_order_id);
            return Ok(ExecutionReadback::Rejected);
        }
        if order.state != OrderState::Filled || order.filled_quantity != request.quantity {
            return Ok(ExecutionReadback::Accepted);
        }
        let Some(before_position) = self
            .pre_dispatch_positions
            .get(&request.client_order_id)
            .copied()
        else {
            // A process restart has no in-memory pre-send position. Keep the durable command
            // accepted until the account projection can establish the next safe target.
            return Ok(ExecutionReadback::Accepted);
        };
        let after = self.snapshot(request, &credentials).await?;
        let result = converged(request, request.quantity, before_position, &after, &order);
        if result == ExecutionReadback::Reconciled {
            self.pre_dispatch_positions.remove(&request.client_order_id);
        }
        Ok(result)
    }
}

impl BinanceExecution for BinanceExecutionRouter {
    async fn submit(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError> {
        self.exchange(request)?.submit(request, credentials).await
    }

    async fn readback(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError> {
        self.exchange(request)?.readback(request, credentials).await
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

async fn rules_for_request(
    transport: &BinanceHttpTransport,
    request: &ExecutionRequest,
    _readback: &venue_gateway_binance::BinancePrivateReadbackCandidate,
) -> Result<venue_gateway_binance::BinanceInstrumentRules, BinanceExecutionError> {
    let response = transport
        .fetch_usd_m_exchange_info()
        .await
        .map_err(|_| BinanceExecutionError::Unavailable)?;
    let payload =
        std::str::from_utf8(&response.payload).map_err(|_| BinanceExecutionError::Unavailable)?;
    parse_instrument_rules(
        payload,
        request.symbol.clone(),
        transport.instrument_generation(),
    )
    .map_err(|_| BinanceExecutionError::Invalid)
}

fn validate_request_binding(
    transport: &BinanceHttpTransport,
    request: &ExecutionRequest,
) -> Result<(), BinanceExecutionError> {
    let binding = transport.config().gateway_binding();
    if request.command_id.is_empty()
        || request.client_order_id.is_empty()
        || request.trading_account_id != binding.trading_account_id
        || request.symbol != binding.symbol
        || request.position_side == PositionSide::Net
        || request.quantity <= Decimal::ZERO
        || request.reducing
            != matches!(
                (request.position_side, request.side),
                (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy)
            )
    {
        return Err(BinanceExecutionError::Invalid);
    }
    Ok(())
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

fn converged(
    request: &ExecutionRequest,
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
        .find(|position| position.side == request.position_side)
        .map(|position| position.quantity);
    let expected = if request.reducing {
        before_quantity.checked_sub(quantity)
    } else {
        before_quantity.checked_add(quantity)
    };
    if fills == Some(quantity) && after_quantity == expected {
        ExecutionReadback::Reconciled
    } else {
        // Filled is still not complete until accountTradeList and position risk agree. This also
        // keeps partial fills from being presented as a completed copy command.
        ExecutionReadback::Accepted
    }
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

fn now_ms() -> Result<u64, BinanceExecutionError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceExecutionError::Unavailable)?
        .as_millis();
    u64::try_from(value).map_err(|_| BinanceExecutionError::Unavailable)
}

#[derive(Default)]
pub struct MockBinanceExecution {
    orders: BTreeMap<String, ExecutionReadback>,
    baselines: BTreeMap<String, AccountBaseline>,
}

impl MockBinanceExecution {
    pub fn set_readback(&mut self, client_order_id: String, state: ExecutionReadback) {
        self.orders.insert(client_order_id, state);
    }

    pub fn set_baseline(&mut self, trading_account_id: String, baseline: AccountBaseline) {
        self.baselines.insert(trading_account_id, baseline);
    }
}

impl BinanceExecution for MockBinanceExecution {
    async fn submit(
        &mut self,
        request: &ExecutionRequest,
        _credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError> {
        if request.client_order_id.is_empty()
            || request.command_id.is_empty()
            || request.trading_account_id.is_empty()
        {
            return Err(BinanceExecutionError::Invalid);
        }
        Ok(*self
            .orders
            .entry(request.client_order_id.clone())
            .or_insert(ExecutionReadback::Accepted))
    }

    async fn readback(
        &mut self,
        request: &ExecutionRequest,
        _credentials: BinanceCredentials,
    ) -> Result<ExecutionReadback, BinanceExecutionError> {
        self.orders
            .get(&request.client_order_id)
            .copied()
            .ok_or(BinanceExecutionError::Unavailable)
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
mod tests {
    use secrecy::SecretString;

    use super::*;

    fn credentials() -> Result<BinanceCredentials, Box<dyn std::error::Error>> {
        Ok(BinanceCredentials::from_secrets(
            SecretString::from("a".repeat(32)),
            SecretString::from("b".repeat(32)),
        )?)
    }

    #[tokio::test]
    async fn mock_submit_is_stable_by_client_order_id() -> Result<(), Box<dyn std::error::Error>> {
        let request = ExecutionRequest {
            command_id: "command-a".into(),
            client_order_id: "client-a".into(),
            trading_account_id: "account-a".into(),
            symbol: "BTC/USDT".parse()?,
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 3),
            reducing: false,
        };
        let mut exchange = MockBinanceExecution::default();
        assert_eq!(
            exchange.submit(&request, credentials()?).await?,
            ExecutionReadback::Accepted
        );
        exchange.set_readback("client-a".into(), ExecutionReadback::Reconciled);
        assert_eq!(
            exchange.submit(&request, credentials()?).await?,
            ExecutionReadback::Reconciled
        );
        assert_eq!(
            exchange.readback(&request, credentials()?).await?,
            ExecutionReadback::Reconciled
        );
        Ok(())
    }

    #[test]
    fn exchange_info_rules_floor_quantity_and_reject_too_small_market_commands()
    -> Result<(), Box<dyn std::error::Error>> {
        let rules = parse_instrument_rules(
            include_str!(
                "../../../crates/venue-gateway-binance/tests/fixtures/exchange_info_btcusdt.json"
            ),
            "BTC/USDT".parse()?,
            7,
        )?;
        assert_eq!(
            normalize_quantity(Decimal::new(29, 4), &rules)?,
            Decimal::new(2, 3)
        );
        assert_eq!(
            normalize_quantity(Decimal::new(9, 4), &rules),
            Err(BinanceExecutionError::Invalid)
        );
        Ok(())
    }
}
