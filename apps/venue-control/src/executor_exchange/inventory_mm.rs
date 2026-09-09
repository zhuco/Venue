use super::*;
use venue_control_protocol::kol::{
    ExecutorCommandOrigin, TerminalAccountProjection, TerminalPositionMode,
};
use venue_gateway_binance::{BinanceCancelIntent, BinanceHedgeDispatchFence};

impl BinanceHttpExecution {
    pub(super) async fn submit_mm_stream(
        &mut self,
        request: &ExecutionRequest,
        projection: Option<&TerminalAccountProjection>,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        validate_request_binding(&self.transport, request)?;
        if request.origin != ExecutorCommandOrigin::InventoryMm {
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
        let generation = projection.map_or(self.transport.private_generation(), |p| {
            p.private_generation
        });
        self.transport
            .rebind_generations(rules.instrument.generation, generation)
            .map_err(|_| BinanceExecutionError::Invalid)?;
        let fence = BinanceHedgeDispatchFence::new(
            self.transport.config(),
            rules.clone(),
            generation,
            self.next_attempt_id,
            now_ms()?,
        )
        .map_err(BinanceExecutionError::from)?;
        self.next_attempt_id = self
            .next_attempt_id
            .checked_add(1)
            .ok_or(BinanceExecutionError::Unavailable)?;
        let prepared = match &request.order_kind {
            ExecutionOrderKind::Limit {
                side,
                position_side,
                quantity,
                price,
                reducing,
                time_in_force: LimitTimeInForce::PostOnly,
            } => {
                let projection = projection.ok_or(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::AccountFacts,
                ))?;
                let available = mm_available_quantity(request, projection, now_ms()?)?;
                let quantity = normalize_quantity(
                    if *reducing {
                        quantity.min(&available).to_owned()
                    } else {
                        *quantity
                    },
                    &rules,
                )?;
                fence
                    .prepare_place_limit(&BinancePlaceIntent {
                        client_order_id: request.client_order_id.clone(),
                        side: *side,
                        position_side: *position_side,
                        quantity,
                        limit_price: venue_domain::Price::new(*price)
                            .map_err(|_| BinanceExecutionError::Invalid)?,
                        time_in_force: BinanceTimeInForce::PostOnly,
                        reduce_only: *reducing,
                    })
                    .map_err(BinanceExecutionError::from)?
            }
            ExecutionOrderKind::CancelExact {
                native_order_id: Some(_),
                target_client_order_id: Some(client),
            } => {
                // The durable cancel gate already proved this exact client's instance ownership.
                // Cancel need not wait for a healthy inventory projection during stream recovery.
                fence
                    .prepare_cancel(&BinanceCancelIntent {
                        client_order_id: client.clone(),
                    })
                    .map_err(BinanceExecutionError::from)?
            }
            _ => return Err(BinanceExecutionError::Invalid),
        };
        let response = self
            .transport
            .dispatch_once(
                &credentials,
                fence.scope(),
                &prepared,
                self.transport
                    .signing_timestamp_ms()
                    .map_err(|_| BinanceExecutionError::Unavailable)?,
            )
            .await;
        match response {
            Ok(ack) => Ok(mm_result(
                request,
                &rules,
                &ack.order_id,
                ack.order.as_ref(),
            )),
            Err(error) => dispatch_failed(error, request.known_native_order_id.clone()),
        }
    }
}

fn mm_available_quantity(
    request: &ExecutionRequest,
    projection: &TerminalAccountProjection,
    now: u64,
) -> Result<Decimal, BinanceExecutionError> {
    let invalid = || BinanceExecutionError::PreDispatch(PreDispatchRejection::AccountFacts);
    projection.validate().map_err(|_| invalid())?;
    if projection.credential_id != request.credential_id
        || projection.trading_account_id != request.trading_account_id
        || projection.position_mode != TerminalPositionMode::Hedge
        || projection.private_generation == 0
        || projection.observed_ms > now
        || now - projection.observed_ms > 5_000
        || projection
            .conditional_orders
            .iter()
            .any(|o| o.symbol == request.symbol)
    {
        return Err(invalid());
    }
    let (side, leg, _, reducing) = place_shape(request)?;
    if !reducing {
        return Ok(Decimal::ZERO);
    }
    let mut positions = projection
        .positions
        .iter()
        .filter(|p| p.symbol == request.symbol && p.position_side == leg);
    let quantity = positions.next().map_or(Decimal::ZERO, |p| p.quantity);
    if positions.next().is_some() {
        return Err(invalid());
    }
    let mut reserved = projection
        .open_orders
        .iter()
        .filter(|o| o.symbol == request.symbol && o.position_side == leg && o.order_side == side)
        .try_fold(Decimal::ZERO, |sum, order| {
            let remaining = order
                .quantity
                .checked_sub(order.filled_quantity.ok_or_else(invalid)?)
                .filter(|v| *v >= Decimal::ZERO)
                .ok_or_else(invalid)?;
            sum.checked_add(remaining).ok_or_else(invalid)
        })?;
    let mut seen = BTreeSet::new();
    for reservation in &request.reconciled_close_reservations {
        if reservation.credential_id != request.credential_id
            || reservation.trading_account_id != request.trading_account_id
            || reservation.symbol != request.symbol
            || reservation.position_side != leg
            || reservation.side != side
            || reservation.quantity < Decimal::ZERO
            || !seen.insert(&reservation.client_order_id)
        {
            return Err(invalid());
        }
        if !projection
            .open_orders
            .iter()
            .any(|o| o.client_order_id == reservation.client_order_id)
        {
            reserved = reserved
                .checked_add(reservation.quantity)
                .ok_or_else(invalid)?;
        }
    }
    quantity
        .checked_sub(reserved)
        .filter(|v| *v >= Decimal::ZERO)
        .ok_or_else(invalid)
}

fn mm_result(
    request: &ExecutionRequest,
    rules: &venue_gateway_binance::BinanceInstrumentRules,
    native: &str,
    order: Option<&venue_domain::Order>,
) -> ExecutionOutcome {
    let unknown = || outcome(ExecutionReadback::Unknown, Some(native.to_owned()));
    let Some(order) = order else {
        return unknown();
    };
    if order.validate().is_err() || order.order_id != native || order.symbol != request.symbol {
        return unknown();
    }
    match &request.order_kind {
        ExecutionOrderKind::Limit { .. }
            if exact_place_matches(request, order, rules) == Ok(true) =>
        {
            mirror_order_outcome(order, false)
        }
        ExecutionOrderKind::CancelExact {
            native_order_id: Some(native),
            target_client_order_id: Some(client),
        } if order.order_id == *native
            && order.client_order_id == FieldState::Known(client.clone()) =>
        {
            mirror_order_outcome(order, true)
        }
        _ => unknown(),
    }
}

#[cfg(test)]
#[path = "inventory_mm_tests.rs"]
mod tests;
