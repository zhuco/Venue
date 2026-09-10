use super::*;

// Display/history recovery owns its own cursor. A pooled execution connection must never
// accumulate a fill backlog across unrelated commands, or one full page poisons every retry.
pub(super) fn execution_fills_cursor(request: &ExecutionRequest, now: u64) -> RecentFillsCursor {
    RecentFillsCursor {
        observed_through_ms: request
            .market_baseline
            .as_ref()
            .map_or(now, |baseline| baseline.observed_ms)
            .saturating_sub(1),
        last_trade_id: None,
        last_event_time_ms: None,
    }
}

impl BinanceHttpExecution {
    pub(super) async fn exact_read_scope(
        &mut self,
        request: &ExecutionRequest,
    ) -> Result<
        (
            BinancePrivateReadScope,
            venue_gateway_binance::BinanceInstrumentRules,
        ),
        BinanceExecutionError,
    > {
        validate_request_binding(&self.transport, request)?;
        if self.transport.signing_timestamp_ms().is_err() {
            self.transport
                .synchronize_clock()
                .await
                .map_err(|_| BinanceExecutionError::Unavailable)?;
        }
        let (rules, _) = self
            .catalogue
            .view(&self.transport, &request.symbol)
            .await?;
        let attempt_id = self.next_attempt_id;
        self.next_attempt_id = attempt_id
            .checked_add(1)
            .ok_or(BinanceExecutionError::Unavailable)?;
        let scope = BinancePrivateReadScope::new(
            self.transport.config(),
            &rules,
            self.transport.private_generation(),
            attempt_id,
            now_ms()?,
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        Ok((scope, rules))
    }

    pub(super) async fn readback_limit_exact(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        let (scope, rules) = self.exact_read_scope(request).await?;
        let order = self
            .exact_order_for_client_in_scope(request, credentials, &request.client_order_id, &scope)
            .await?;
        if request
            .known_native_order_id
            .as_ref()
            .is_some_and(|id| id != &order.order_id)
            || !exact_place_matches(request, &order, &rules)?
        {
            return Ok(outcome(
                ExecutionReadback::Unknown,
                request.known_native_order_id.clone(),
            ));
        }
        Ok(limit_outcome(request, &order))
    }
}

fn limit_outcome(
    request: &ExecutionRequest,
    order: &venue_domain::domain::Order,
) -> ExecutionOutcome {
    if let Some(result) = signed_limit_outcome(request, order) {
        return result;
    }
    // A validated exact limit order proves placement and cumulative execution independently
    // of unrelated trades or the account's subsequent position. Missing identity stays unknown.
    let mut result = mirror_order_outcome(order, false);
    if result.state == ExecutionReadback::Unknown {
        return result;
    }
    result.state = match place_readback_decision(order.state, order.filled_quantity) {
        PlaceReadbackDecision::Unknown => ExecutionReadback::Unknown,
        PlaceReadbackDecision::Rejected => ExecutionReadback::Rejected,
        PlaceReadbackDecision::Accepted => ExecutionReadback::Accepted,
        PlaceReadbackDecision::VerifyTerminal => result.state,
    };
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrelated_commands_start_current_while_market_recovery_retains_its_baseline()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut request = super::super::tests::grid_place_request(0)?;
        assert_eq!(
            execution_fills_cursor(&request, 50_000).observed_through_ms,
            49_999
        );
        request.market_baseline = Some(MarketBaseline {
            before_quantity: Decimal::ONE,
            order_quantity: Decimal::ONE,
            observed_ms: 1_000,
            valid_until_ms: 2_000,
        });
        assert_eq!(
            execution_fills_cursor(&request, 50_000).observed_through_ms,
            999
        );
        request.market_baseline = None;
        assert_eq!(
            execution_fills_cursor(&request, 90_000).observed_through_ms,
            89_999
        );
        Ok(())
    }

    #[test]
    fn exact_terminal_limit_converges_without_account_position_or_fill_history()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = super::super::tests::grid_place_request(0)?;
        let mut order = venue_domain::domain::Order {
            order_id: "native".into(),
            client_order_id: FieldState::Known(request.client_order_id.clone()),
            symbol: request.symbol.clone(),
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            purpose: FieldState::Missing,
            state: OrderState::Filled,
            quantity: Decimal::ONE,
            filled_quantity: Decimal::ONE,
            limit_price: None,
            time_in_force: FieldState::Known(LimitTimeInForce::PostOnly),
            average_price: FieldState::Missing,
            reduce_only: false,
        };
        assert_eq!(
            limit_outcome(&request, &order).state,
            ExecutionReadback::Reconciled
        );
        order.state = OrderState::Cancelled;
        order.filled_quantity = Decimal::new(5, 1);
        assert_eq!(
            limit_outcome(&request, &order).state,
            ExecutionReadback::Reconciled
        );
        order.filled_quantity = Decimal::ZERO;
        assert_eq!(
            limit_outcome(&request, &order).state,
            ExecutionReadback::Rejected
        );
        order.state = OrderState::New;
        assert_eq!(
            limit_outcome(&request, &order).state,
            ExecutionReadback::Accepted
        );
        order.state = OrderState::Unknown;
        assert_eq!(
            limit_outcome(&request, &order).state,
            ExecutionReadback::Unknown
        );
        Ok(())
    }
}
