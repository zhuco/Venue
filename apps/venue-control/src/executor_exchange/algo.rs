use super::*;
use venue_gateway_binance::private::{AlgoOrderReadback, ConditionalStrategyStatus};

impl BinanceHttpExecution {
    pub(super) async fn submit_algo_request(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        let opening = matches!(
            request.order_kind,
            ExecutionOrderKind::StopMarket {
                reducing: false,
                ..
            }
        );
        let (before, rules, risk) = self.read_snapshot(request, &credentials, opening).await?;
        let prepared = match &request.order_kind {
            ExecutionOrderKind::StopMarket {
                side,
                position_side,
                quantity,
                trigger_price,
                working_type,
                reducing,
            } => {
                let quantity = if *reducing {
                    let available = position_quantity(&before, *position_side)?
                        .checked_sub(reserved_close_quantity(
                            &before,
                            request,
                            *position_side,
                            *side,
                        )?)
                        .ok_or(BinanceExecutionError::Invalid)?;
                    normalize_quantity((*quantity).min(available.max(Decimal::ZERO)), &rules)?
                } else {
                    let context = request
                        .copy_risk
                        .as_ref()
                        .ok_or(BinanceExecutionError::Invalid)?;
                    copy_risk::check_mirror_limit_risk(
                        context,
                        self.transport.config().gateway_binding(),
                        risk.as_ref().ok_or(BinanceExecutionError::Invalid)?,
                        &rules,
                        *quantity,
                        *trigger_price,
                        now_ms()?,
                    )?;
                    copy_risk::normalize_copy_open_quantity(
                        context,
                        *quantity,
                        *trigger_price,
                        &rules,
                    )?
                };
                prepare_place_stop_market(
                    &rules,
                    &before,
                    &BinanceStopMarketIntent {
                        client_order_id: request.client_order_id.clone(),
                        side: *side,
                        position_side: *position_side,
                        quantity,
                        trigger_price: venue_domain::Price::new(*trigger_price)
                            .map_err(|_| BinanceExecutionError::Invalid)?,
                        working_type: working_type.clone(),
                        reduce_only: *reducing,
                    },
                )
            }
            ExecutionOrderKind::CancelAlgoExact {
                native_order_id,
                target_client_order_id,
            } => {
                let (native, client) = select_algo(
                    before.algo_custody(),
                    native_order_id.as_deref(),
                    target_client_order_id.as_deref(),
                )?
                .ok_or(BinanceExecutionError::Unavailable)?;
                if request
                    .known_native_order_id
                    .as_ref()
                    .is_some_and(|known| known != &native)
                {
                    return Err(BinanceExecutionError::Unavailable);
                }
                prepare_cancel_algo(&rules, &before, &native, &client)
            }
            _ => return Err(BinanceExecutionError::Invalid),
        }
        .map_err(|_| BinanceExecutionError::Invalid)?;
        match self
            .transport
            .dispatch_then_exact_readback(&credentials, before.scope(), &prepared, now_ms()?)
            .await
        {
            BinancePhysicalMutationOutcome::DispatchUnknown { error } => Ok(dispatch_unknown(
                error,
                request.known_native_order_id.clone(),
            )),
            BinancePhysicalMutationOutcome::DispatchFailed { error } => {
                dispatch_failed(error, request.known_native_order_id.clone())
            }
            BinancePhysicalMutationOutcome::AckedReadbackUnknown { ack, .. } => {
                Ok(outcome(ExecutionReadback::Unknown, Some(ack.order_id)))
            }
            BinancePhysicalMutationOutcome::ReadBack { ack, readback } => {
                if ack.order_id != readback.order.order_id {
                    return Ok(outcome(ExecutionReadback::Unknown, Some(ack.order_id)));
                }
                let Some(algo) = readback.algo.as_ref() else {
                    return Ok(outcome(ExecutionReadback::Unknown, Some(ack.order_id)));
                };
                Ok(algo_outcome(request, algo, &rules))
            }
        }
    }

    pub(super) async fn readback_algo_request(
        &mut self,
        request: &ExecutionRequest,
        credentials: BinanceCredentials,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        let (snapshot, rules) = self.snapshot_with_rules(request, &credentials).await?;
        let client = match &request.order_kind {
            ExecutionOrderKind::StopMarket { .. } => request.client_order_id.as_str(),
            ExecutionOrderKind::CancelAlgoExact {
                target_client_order_id: Some(client),
                ..
            } => client,
            _ => return Err(BinanceExecutionError::Invalid),
        };
        let algo = self
            .exact_algo_for_client_in_scope(&credentials, client, snapshot.scope())
            .await?;
        Ok(algo_outcome(request, &algo, &rules))
    }
}

fn select_algo(
    orders: &[AlgoOrderReadback],
    native: Option<&str>,
    client: Option<&str>,
) -> Result<Option<(String, String)>, BinanceExecutionError> {
    if native.is_none() && client.is_none() {
        return Err(BinanceExecutionError::Invalid);
    }
    let mut selected = None;
    for order in orders {
        if native.is_none_or(|value| value == order.algo_id)
            && client.is_none_or(|value| value == order.client_algo_id)
        {
            if selected.is_some() {
                return Err(BinanceExecutionError::Unavailable);
            }
            selected = Some((order.algo_id.clone(), order.client_algo_id.clone()));
        }
    }
    Ok(selected)
}

fn algo_outcome(
    request: &ExecutionRequest,
    algo: &AlgoOrderReadback,
    rules: &venue_gateway_binance::BinanceInstrumentRules,
) -> ExecutionOutcome {
    let identity_matches = request
        .known_native_order_id
        .as_ref()
        .is_none_or(|native| native == &algo.algo_id)
        && (algo.client_algo_id == request.client_order_id
            || matches!(
                &request.order_kind,
                ExecutionOrderKind::CancelAlgoExact {
                    target_client_order_id: Some(client),
                    ..
                } if client == &algo.client_algo_id
            ));
    let terms_match = match &request.order_kind {
        ExecutionOrderKind::StopMarket {
            side,
            position_side,
            quantity,
            trigger_price,
            working_type,
            reducing: _,
        } => {
            let normalized = match request
                .copy_risk
                .as_ref()
                .filter(|risk| risk.round_open_quantity_up || risk.open_quantity_rounding.is_some())
            {
                Some(risk) => {
                    copy_risk::normalize_copy_open_quantity(risk, *quantity, *trigger_price, rules)
                }
                None => normalize_quantity(*quantity, rules),
            };
            normalized.is_ok_and(|quantity| algo.quantity == FieldState::Known(quantity))
                && algo.order_type == FieldState::Known("STOP_MARKET".to_owned())
                && algo.side == FieldState::Known(*side)
                && algo.position_side == FieldState::Known(*position_side)
                && matches!(algo.trigger_price, FieldState::Known(price) if price.value() == *trigger_price)
                && algo.working_type == FieldState::Known(working_type.clone())
        }
        ExecutionOrderKind::CancelAlgoExact { .. } => true,
        _ => false,
    };
    let matches = identity_matches && terms_match;
    let state = match (&request.order_kind, algo.status) {
        (ExecutionOrderKind::StopMarket { .. }, ConditionalStrategyStatus::Current) if matches => {
            ExecutionReadback::Reconciled
        }
        (
            ExecutionOrderKind::CancelAlgoExact { .. },
            ConditionalStrategyStatus::Cancelled | ConditionalStrategyStatus::NonCancelledTerminal,
        ) if matches => ExecutionReadback::Reconciled,
        (_, ConditionalStrategyStatus::Rejected) if matches => ExecutionReadback::Rejected,
        _ => ExecutionReadback::Unknown,
    };
    let mut result = outcome(state, Some(algo.algo_id.clone()));
    if state == ExecutionReadback::Reconciled {
        let quantity = match algo.quantity {
            FieldState::Known(value) => value,
            _ => Decimal::ZERO,
        };
        result.order_fact = (quantity > Decimal::ZERO).then_some(ExactOrderFact {
            quantity,
            filled_quantity: Decimal::ZERO,
            terminal: !matches!(algo.status, ConditionalStrategyStatus::Current),
        });
    }
    result
}
