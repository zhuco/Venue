use super::*;

pub(super) fn validate_request_binding(
    transport: &BinanceHttpTransport,
    request: &ExecutionRequest,
) -> Result<(), BinanceExecutionError> {
    let binding = transport.config().gateway_binding();
    if request.command_id.is_empty()
        || request.client_order_id.is_empty()
        || request.credential_id.is_empty()
        || request
            .known_native_order_id
            .as_deref()
            .is_some_and(invalid_native_order_id)
    {
        return Err(BinanceExecutionError::PreDispatch(
            PreDispatchRejection::Identity,
        ));
    }
    if request.trading_account_id != binding.trading_account_id || request.symbol != binding.symbol
    {
        return Err(BinanceExecutionError::PreDispatch(
            PreDispatchRejection::Binding,
        ));
    }
    match &request.order_kind {
        ExecutionOrderKind::Market {
            side,
            position_side,
            quantity,
            reducing,
        }
        | ExecutionOrderKind::Limit {
            side,
            position_side,
            quantity,
            reducing,
            ..
        } => {
            let price_invalid = matches!(
                &request.order_kind,
                ExecutionOrderKind::Limit { price, .. } if *price <= Decimal::ZERO
            );
            let unsupported_gtc = matches!(
                &request.order_kind,
                ExecutionOrderKind::Limit {
                    time_in_force: LimitTimeInForce::Gtc,
                    ..
                }
            ) && request.origin
                != venue_control_protocol::kol::ExecutorCommandOrigin::Copy;
            if *quantity <= Decimal::ZERO {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::Quantity,
                ));
            }
            if price_invalid {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::Price,
                ));
            }
            if unsupported_gtc {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::OrderType,
                ));
            }
            if !*reducing && !request.reconciled_close_reservations.is_empty() {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::CloseReservation,
                ));
            }
            if *position_side == PositionSide::Net
                || *reducing
                    != matches!(
                        (*position_side, *side),
                        (PositionSide::Long, OrderSide::Sell)
                            | (PositionSide::Short, OrderSide::Buy)
                    )
            {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::Direction,
                ));
            }
        }
        ExecutionOrderKind::StopMarket {
            side,
            position_side,
            quantity,
            trigger_price,
            working_type,
            reducing,
        } => {
            if *quantity <= Decimal::ZERO {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::Quantity,
                ));
            }
            if *trigger_price <= Decimal::ZERO {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::Price,
                ));
            }
            if !matches!(working_type.as_str(), "MARK_PRICE" | "CONTRACT_PRICE") {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::OrderType,
                ));
            }
            if *position_side == PositionSide::Net
                || *reducing
                    != matches!(
                        (*position_side, *side),
                        (PositionSide::Long, OrderSide::Sell)
                            | (PositionSide::Short, OrderSide::Buy)
                    )
            {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::Direction,
                ));
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
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::CancelTarget,
                ));
            }
        }
        ExecutionOrderKind::CancelAlgoExact {
            native_order_id,
            target_client_order_id,
        } => {
            if native_order_id.is_none() && target_client_order_id.is_none()
                || native_order_id
                    .as_deref()
                    .is_some_and(invalid_native_order_id)
                || target_client_order_id
                    .as_deref()
                    .is_some_and(invalid_native_order_id)
            {
                return Err(BinanceExecutionError::PreDispatch(
                    PreDispatchRejection::CancelTarget,
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_validation_preserves_the_specific_failed_condition()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut valid = super::super::tests::grid_place_request(0)?;
        valid.trading_account_id = "00000000-0000-4000-8000-000000000001".into();
        let router =
            BinanceExecutionRouter::new(venue_gateway_binance::BinanceTransportLimits::new(
                std::time::Duration::from_secs(1),
                1024,
            )?);
        let exchange = router.account_exchange(&valid.trading_account_id, &valid.symbol)?;
        let exchange = exchange.try_lock()?;
        let check = |request: &ExecutionRequest| {
            validate_request_binding(&exchange.transport, request)
                .map_err(BinanceExecutionError::not_dispatched_code)
        };
        assert_eq!(check(&valid), Ok(()));
        let mut request = valid.clone();
        request.trading_account_id = "other-account".into();
        assert_eq!(check(&request), Err("not_dispatched_binding"));
        request = valid.clone();
        request.command_id.clear();
        assert_eq!(check(&request), Err("not_dispatched_identity"));
        for (quantity, price, side, position_side, tif, expected) in [
            (
                Decimal::ZERO,
                Decimal::ONE,
                OrderSide::Buy,
                PositionSide::Long,
                LimitTimeInForce::PostOnly,
                "not_dispatched_quantity",
            ),
            (
                Decimal::ONE,
                Decimal::ZERO,
                OrderSide::Buy,
                PositionSide::Long,
                LimitTimeInForce::PostOnly,
                "not_dispatched_price",
            ),
            (
                Decimal::ONE,
                Decimal::ONE,
                OrderSide::Sell,
                PositionSide::Long,
                LimitTimeInForce::PostOnly,
                "not_dispatched_direction",
            ),
            (
                Decimal::ONE,
                Decimal::ONE,
                OrderSide::Buy,
                PositionSide::Net,
                LimitTimeInForce::PostOnly,
                "not_dispatched_direction",
            ),
            (
                Decimal::ONE,
                Decimal::ONE,
                OrderSide::Buy,
                PositionSide::Long,
                LimitTimeInForce::Gtc,
                "not_dispatched_order_type",
            ),
        ] {
            request = valid.clone();
            request.order_kind = ExecutionOrderKind::Limit {
                side,
                position_side,
                quantity,
                price,
                reducing: false,
                time_in_force: tif,
            };
            assert_eq!(check(&request), Err(expected));
        }
        request = valid.clone();
        request.order_kind = ExecutionOrderKind::CancelExact {
            native_order_id: None,
            target_client_order_id: None,
        };
        assert_eq!(check(&request), Err("not_dispatched_cancel_target"));
        // Manual opening does not impose minimum notional or account-position prerequisites.
        request = valid;
        request.order_kind = ExecutionOrderKind::Limit {
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 3),
            price: Decimal::ONE,
            reducing: false,
            time_in_force: LimitTimeInForce::PostOnly,
        };
        assert_eq!(check(&request), Ok(()));
        Ok(())
    }
}
