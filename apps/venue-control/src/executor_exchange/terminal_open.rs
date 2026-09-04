use super::*;
use venue_control_protocol::kol::ExecutorCommandOrigin;

pub(crate) fn is_terminal_open(request: &ExecutionRequest) -> bool {
    request.origin == ExecutorCommandOrigin::Terminal
        && matches!(
            request.order_kind,
            ExecutionOrderKind::LimitPostOnly {
                reducing: false,
                ..
            }
        )
}

impl BinanceHttpExecution {
    pub(super) async fn submit_terminal_open(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        validate_request_binding(&self.transport, request)?;
        if !is_terminal_open(request) {
            return Err(BinanceExecutionError::Invalid);
        }
        // Initial public-only warmup is required; subsequent opens use the same pool and clock.
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
        let ExecutionOrderKind::LimitPostOnly {
            side,
            position_side,
            quantity,
            price,
            reducing: false,
        } = request.order_kind
        else {
            return Err(BinanceExecutionError::Invalid);
        };
        let quantity = quantity - quantity % rules.instrument.quantity_step;
        if quantity <= Decimal::ZERO {
            return Err(BinanceExecutionError::OpenQuantityZero);
        }
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
        let prepared = venue_gateway_binance::prepare_terminal_open_limit(
            &rules,
            &scope,
            &BinancePlaceIntent {
                client_order_id: request.client_order_id.clone(),
                side,
                position_side,
                quantity,
                limit_price: venue_domain::domain::Price::new(price)
                    .map_err(|_| BinanceExecutionError::Invalid)?,
                time_in_force: BinanceTimeInForce::PostOnly,
                reduce_only: false,
            },
        )
        .map_err(|_| BinanceExecutionError::Invalid)?;
        let started = Instant::now();
        let response = self
            .transport
            .dispatch_once(
                credentials,
                &scope,
                &prepared,
                self.transport
                    .signing_timestamp_ms()
                    .map_err(|_| BinanceExecutionError::Unavailable)?,
            )
            .await;
        tracing::info!(target: "venue_control::terminal", command_id = %request.command_id,
            dispatch_elapsed_us = elapsed_us(started), "Manual opening dispatch completed");
        match response {
            Ok(ack) => Ok(grid_batch::grid_result_outcome(request, &rules, &ack)),
            Err(error) => dispatch_failed(error, None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fast_path_is_only_manual_limit_opening() -> Result<(), Box<dyn std::error::Error>> {
        let mut request = ExecutionRequest {
            origin: ExecutorCommandOrigin::Terminal,
            command_id: "command".into(),
            client_order_id: "client".into(),
            credential_id: "credential".into(),
            trading_account_id: "account".into(),
            symbol: "DOGE/USDC".parse()?,
            order_kind: ExecutionOrderKind::LimitPostOnly {
                side: OrderSide::Buy,
                position_side: PositionSide::Long,
                quantity: Decimal::from(288),
                price: Decimal::new(8663, 5),
                reducing: false,
            },
            known_native_order_id: None,
            reconciled_close_reservations: Vec::new(),
        };
        assert!(is_terminal_open(&request));
        for origin in [ExecutorCommandOrigin::Grid, ExecutorCommandOrigin::Copy] {
            request.origin = origin;
            assert!(!is_terminal_open(&request));
        }
        request.origin = ExecutorCommandOrigin::Terminal;
        if let ExecutionOrderKind::LimitPostOnly { reducing, .. } = &mut request.order_kind {
            *reducing = true;
        }
        assert!(!is_terminal_open(&request));
        request.order_kind = ExecutionOrderKind::Market {
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::ONE,
            reducing: false,
        };
        assert!(!is_terminal_open(&request));
        Ok(())
    }

    #[test]
    fn manual_open_does_not_pull_private_surfaces() {
        let source = include_str!("terminal_open.rs")
            .split("#[cfg(test)]")
            .next()
            .unwrap_or("");
        assert!(source.contains("dispatch_once"));
        for forbidden in [
            "snapshot_with_rules",
            "execute_read",
            "position_quantity",
            "check_minimum_notional",
            "dispatch_then_exact_readback",
        ] {
            assert!(
                !source.contains(forbidden),
                "unexpected private preflight: {forbidden}"
            );
        }
    }
}
