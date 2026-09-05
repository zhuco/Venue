use super::*;
use serde::{Deserialize, Serialize};
use venue_control_protocol::kol::TerminalPosition;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalMarketContext {
    #[serde(with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    pub private_generation: u64,
    pub observed_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TerminalPositionSettlement {
    #[serde(with = "rust_decimal::serde::str")]
    pub executed_quantity: Decimal,
    pub positions: Vec<TerminalPosition>,
    pub observed_ms: u64,
}

pub struct TerminalMarketResult {
    pub outcome: ExecutionOutcome,
    pub settlement: Option<TerminalPositionSettlement>,
    pub failure_code: Option<&'static str>,
}

pub type TerminalMarketFuture<'a> =
    Pin<Box<dyn Future<Output = Result<TerminalMarketResult, BinanceExecutionError>> + Send + 'a>>;

impl TerminalMarketResult {
    fn uncertain(native_order_id: Option<String>) -> Self {
        Self {
            outcome: outcome(ExecutionReadback::Unknown, native_order_id),
            settlement: None,
            failure_code: None,
        }
    }
}

impl BinanceHttpExecution {
    pub(super) async fn terminal_market_request(
        &mut self,
        request: &ExecutionRequest,
        context: &TerminalMarketContext,
        credentials: BinanceCredentials,
        read_only: bool,
    ) -> Result<TerminalMarketResult, BinanceExecutionError> {
        validate_request_binding(&self.transport, request)?;
        let ExecutionOrderKind::Market {
            side,
            position_side,
            reducing,
            ..
        } = request.order_kind
        else {
            return Err(BinanceExecutionError::Invalid);
        };
        if request.origin != venue_control_protocol::kol::ExecutorCommandOrigin::Terminal
            || context.quantity <= Decimal::ZERO
            || context.private_generation == 0
        {
            return Err(BinanceExecutionError::Invalid);
        }
        if self.transport.signing_timestamp_ms().is_err() {
            self.transport
                .synchronize_clock()
                .await
                .map_err(|_| BinanceExecutionError::Unavailable)?;
        }
        let rules = self
            .catalogue
            .rules(&self.transport, &request.symbol)
            .await?;
        let scope = BinancePrivateReadScope::new(
            self.transport.config(),
            &rules,
            self.transport.private_generation(),
            self.next_attempt_id,
            now_ms()?,
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(BinanceExecutionError::Unavailable)?;
        let order = if read_only {
            let exact = build_exact_order_request(&scope, &request.client_order_id)
                .map_err(|_| BinanceExecutionError::Invalid)?;
            let page = self
                .transport
                .execute_read(
                    &credentials,
                    &exact,
                    self.transport
                        .signing_timestamp_ms()
                        .map_err(|_| BinanceExecutionError::Unavailable)?,
                )
                .await
                .map_err(|_| BinanceExecutionError::Unavailable)?;
            parse_order(
                std::str::from_utf8(&page.payload)
                    .map_err(|_| BinanceExecutionError::Unavailable)?,
                &request.symbol,
            )
            .ok()
        } else {
            let prepared = venue_gateway_binance::prepare_terminal_market(
                &rules,
                &scope,
                &BinanceMarketIntent {
                    client_order_id: request.client_order_id.clone(),
                    side,
                    position_side,
                    quantity: context.quantity,
                    reduce_only: reducing,
                },
            )
            .map_err(|_| BinanceExecutionError::Invalid)?;
            let timestamp = self
                .transport
                .signing_timestamp_ms()
                .map_err(|_| BinanceExecutionError::Unavailable)?;
            let started = Instant::now();
            let result = self
                .transport
                .dispatch_once(&credentials, &scope, &prepared, timestamp)
                .await;
            tracing::info!(target: "venue_control::terminal", command_id = %request.command_id,
                dispatch_elapsed_us = elapsed_us(started), "Manual position market dispatch completed");
            match result {
                Ok(ack) => ack.order,
                Err(error) => {
                    // After entering POST, only a classified exchange rejection is definitive.
                    let result = dispatch_failed(error, None)?;
                    return Ok(TerminalMarketResult {
                        outcome: result,
                        settlement: None,
                        failure_code: None,
                    });
                }
            }
        };
        let Some(order) = order else {
            return Ok(TerminalMarketResult::uncertain(
                request.known_native_order_id.clone(),
            ));
        };
        if !terminal_order_matches(request, context, &order) {
            return Ok(TerminalMarketResult::uncertain(
                request.known_native_order_id.clone(),
            ));
        }
        let native_id = Some(order.order_id.clone());
        // This is deliberately after POST (or exact-ID recovery), never a preflight read.
        let refreshed = self.refresh_terminal_positions(&scope, &credentials).await;
        let Ok((positions, observed_ms)) = refreshed else {
            return Ok(TerminalMarketResult::uncertain(native_id));
        };
        let settlement = TerminalPositionSettlement {
            executed_quantity: order.filled_quantity,
            positions,
            observed_ms,
        };
        let (state, failure_code) = match order.state {
            OrderState::Filled if order.filled_quantity == context.quantity => {
                (ExecutionReadback::Reconciled, None)
            }
            OrderState::Cancelled | OrderState::Expired | OrderState::Rejected => (
                ExecutionReadback::Rejected,
                Some(if order.filled_quantity > Decimal::ZERO {
                    "market_partial_fill"
                } else {
                    "market_not_filled"
                }),
            ),
            _ => (ExecutionReadback::Unknown, None),
        };
        Ok(TerminalMarketResult {
            outcome: outcome(state, native_id),
            settlement: Some(settlement),
            failure_code,
        })
    }

    async fn refresh_terminal_positions(
        &self,
        scope: &BinancePrivateReadScope,
        credentials: &BinanceCredentials,
    ) -> Result<(Vec<TerminalPosition>, u64), BinanceExecutionError> {
        let observed_ms = now_ms()?;
        let query = build_positions_request(scope).map_err(|_| BinanceExecutionError::Invalid)?;
        let page = self
            .transport
            .execute_read(
                credentials,
                &query,
                self.transport
                    .signing_timestamp_ms()
                    .map_err(|_| BinanceExecutionError::Unavailable)?,
            )
            .await
            .map_err(|_| BinanceExecutionError::Unavailable)?;
        let positions = venue_gateway_binance::parse_signed_hedge_positions(
            std::str::from_utf8(&page.payload).map_err(|_| BinanceExecutionError::Unavailable)?,
            scope.binding(),
        )
        .map_err(|_| BinanceExecutionError::Unavailable)?;
        Ok((
            positions
                .into_iter()
                .map(|position| TerminalPosition {
                    symbol: position.symbol,
                    position_side: position.side,
                    quantity: position.quantity,
                    entry_price: position.entry_price.map(|p| p.value()),
                    mark_price: position.mark_price.map(|p| p.value()),
                })
                .collect(),
            observed_ms,
        ))
    }
}

fn terminal_order_matches(
    request: &ExecutionRequest,
    context: &TerminalMarketContext,
    order: &venue_domain::Order,
) -> bool {
    let ExecutionOrderKind::Market {
        side,
        position_side,
        ..
    } = request.order_kind
    else {
        return false;
    };
    order.validate().is_ok()
        && order.symbol == request.symbol
        && order.side == side
        && order.position_side == FieldState::Known(position_side)
        && order.client_order_id == FieldState::Known(request.client_order_id.clone())
        && order.quantity == context.quantity
        && request
            .known_native_order_id
            .as_ref()
            .is_none_or(|id| id == &order.order_id)
}
