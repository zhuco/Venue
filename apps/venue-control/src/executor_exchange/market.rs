use super::*;

/// Minimal signed pre-send facts, persisted in the existing command ledger before POST. This is
/// not a strategy checkpoint: it proves a single order's position delta after a process restart.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketBaseline {
    pub before_quantity: Decimal,
    pub order_quantity: Decimal,
    pub observed_ms: u64,
    pub valid_until_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSettlement {
    pub executed_quantity: Decimal,
    pub position_quantity: Decimal,
    pub observed_ms: u64,
}

pub type MarketPreparationFuture<'a> = Pin<
    Box<dyn Future<Output = Result<Option<MarketBaseline>, BinanceExecutionError>> + Send + 'a>,
>;

pub(super) struct PreparedMarket {
    request: ExecutionRequest,
    baseline: MarketBaseline,
    before: venue_gateway_binance::BinancePrivateReadbackCandidate,
    rules: venue_gateway_binance::BinanceInstrumentRules,
    reference: Option<venue_gateway_binance::BinanceMarkPrice>,
}

impl BinanceHttpExecution {
    pub(super) async fn prepare_market_request(
        &mut self,
        request: &ExecutionRequest,
        credentials: &BinanceCredentials,
    ) -> Result<Option<MarketBaseline>, BinanceExecutionError> {
        if !matches!(request.order_kind, ExecutionOrderKind::Market { .. }) {
            return Ok(None);
        }
        self.prepared_market = None;
        let (side, position_side, requested, reducing) = place_shape(request)?;
        let (before, rules, risk) = self
            .read_snapshot(
                request,
                credentials,
                !reducing && request.copy_risk.is_some(),
            )
            .await?;
        let before_quantity = position_quantity(&before, position_side)?;
        let available = if reducing {
            (before_quantity - reserved_close_quantity(&before, request, position_side, side)?)
                .max(Decimal::ZERO)
        } else {
            requested
        };
        let reference = if reducing {
            None
        } else {
            Some(self.prices.mark(&self.transport, &request.symbol).await?)
        };
        let order_quantity = match (request.copy_risk.as_ref(), reference.as_ref()) {
            (Some(context), Some(mark)) => {
                let now = now_ms()?;
                if now < before.scope().requested_at_ms()
                    || now - before.scope().requested_at_ms() > 10_000
                {
                    return Err(BinanceExecutionError::Risk(CopyRiskRejection::AccountFacts));
                }
                clip_open_quantity(
                    context,
                    self.transport.config().gateway_binding(),
                    risk.as_ref()
                        .ok_or(BinanceExecutionError::Risk(CopyRiskRejection::AccountFacts))?,
                    mark,
                    &rules,
                    requested,
                    now,
                )?
            }
            _ => normalize_quantity(requested.min(available), &rules)?,
        };
        if let Some(mark) = &reference {
            check_minimum_notional_at_price(mark.price, order_quantity, &rules)?;
        }
        let baseline = MarketBaseline {
            before_quantity,
            order_quantity,
            observed_ms: before.scope().requested_at_ms(),
            valid_until_ms: now_ms()?.saturating_add(1_000),
        };
        self.prepared_market = Some(PreparedMarket {
            request: request.clone(),
            baseline: baseline.clone(),
            before,
            rules,
            reference,
        });
        Ok(Some(baseline))
    }

    pub(super) fn take_prepared_market(
        &mut self,
        request: &ExecutionRequest,
    ) -> Result<
        (
            venue_gateway_binance::BinancePrivateReadbackCandidate,
            venue_gateway_binance::BinanceInstrumentRules,
            Decimal,
            Option<venue_gateway_binance::BinanceMarkPrice>,
        ),
        BinanceExecutionError,
    > {
        let prepared = self
            .prepared_market
            .take()
            .ok_or(BinanceExecutionError::Invalid)?;
        let mut identity = request.clone();
        identity.market_baseline = None;
        if identity != prepared.request
            || request.market_baseline.as_ref() != Some(&prepared.baseline)
            || now_ms()? > prepared.baseline.valid_until_ms
        {
            return Err(BinanceExecutionError::Unavailable);
        }
        let now = now_ms()?;
        if prepared
            .reference
            .as_ref()
            .is_some_and(|mark| now < mark.observed_at_ms || now - mark.observed_at_ms > 5_000)
        {
            return Err(BinanceExecutionError::Risk(CopyRiskRejection::PriceStale));
        }
        Ok((
            prepared.before,
            prepared.rules,
            prepared.baseline.order_quantity,
            prepared.reference,
        ))
    }
}

pub(super) fn signed_market_settlement(
    request: &ExecutionRequest,
    after: &venue_gateway_binance::BinancePrivateReadbackCandidate,
    order: &venue_domain::domain::Order,
) -> Option<MarketSettlement> {
    let baseline = request.market_baseline.as_ref()?;
    let (side, position_side, _, reducing) = place_shape(request).ok()?;
    if !matches!(request.order_kind, ExecutionOrderKind::Market { .. })
        || !terminal_order_state(order.state)
        || order.state == OrderState::Rejected
        || order.quantity != baseline.order_quantity
        || order.filled_quantity <= Decimal::ZERO
        || order.filled_quantity > baseline.order_quantity
        || after.scope().requested_at_ms() <= baseline.observed_ms
        || after
            .regular()
            .orders
            .iter()
            .any(|open| open.order_id == order.order_id)
    {
        return None;
    }
    let mut latest_fill_ms = baseline.observed_ms;
    let executed = after
        .fills()
        .iter()
        .filter(|fill| fill.order_id == order.order_id)
        .try_fold(Decimal::ZERO, |total, fill| {
            if fill.symbol != request.symbol
                || fill.side != side
                || fill.position_side != FieldState::Known(position_side)
                || fill.quantity <= Decimal::ZERO
            {
                return None;
            }
            latest_fill_ms = latest_fill_ms.max(fill.exchange_time_ms?);
            total.checked_add(fill.quantity)
        })?;
    if executed != order.filled_quantity || after.scope().requested_at_ms() < latest_fill_ms {
        return None;
    }
    let expected = expected_market_position(baseline.before_quantity, executed, reducing)?;
    let observed = position_quantity(after, position_side).ok()?;
    if observed != expected {
        return None;
    }
    Some(MarketSettlement {
        executed_quantity: executed,
        position_quantity: observed,
        observed_ms: after.scope().requested_at_ms(),
    })
}

pub(super) fn expected_market_position(
    before: Decimal,
    executed: Decimal,
    reducing: bool,
) -> Option<Decimal> {
    if before < Decimal::ZERO || executed <= Decimal::ZERO {
        return None;
    }
    let next = if reducing {
        before.checked_sub(executed)
    } else {
        before.checked_add(executed)
    }?;
    (next >= Decimal::ZERO).then_some(next)
}

impl MockBinanceExecution {
    pub(super) fn mock_market_baseline(
        &self,
        request: &ExecutionRequest,
    ) -> Result<Option<MarketBaseline>, BinanceExecutionError> {
        if !matches!(request.order_kind, ExecutionOrderKind::Market { .. }) {
            return Ok(None);
        }
        let (_, side, quantity, reducing) = place_shape(request)?;
        let key = format!("{}:{}:{side:?}", request.trading_account_id, request.symbol);
        let before = self
            .market_positions
            .lock()
            .map_err(|_| BinanceExecutionError::Unavailable)?
            .get(&key)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let order_quantity = if reducing {
            quantity.min(before)
        } else {
            quantity
        };
        if order_quantity <= Decimal::ZERO {
            return Err(BinanceExecutionError::Invalid);
        }
        let now = now_ms()?;
        Ok(Some(MarketBaseline {
            before_quantity: before,
            order_quantity,
            observed_ms: now,
            valid_until_ms: now.saturating_add(1_000),
        }))
    }

    pub(super) fn mock_market_settlement(
        &self,
        request: &ExecutionRequest,
        mut result: ExecutionOutcome,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        if result.state != ExecutionReadback::Reconciled {
            return Ok(result);
        }
        let Some(baseline) = request.market_baseline.as_ref() else {
            return Ok(result);
        };
        let (_, side, _, reducing) = place_shape(request)?;
        let position =
            expected_market_position(baseline.before_quantity, baseline.order_quantity, reducing)
                .ok_or(BinanceExecutionError::Invalid)?;
        result.market_settlement = Some(MarketSettlement {
            executed_quantity: baseline.order_quantity,
            position_quantity: position,
            observed_ms: now_ms()?.max(baseline.observed_ms.saturating_add(1)),
        });
        let key = format!("{}:{}:{side:?}", request.trading_account_id, request.symbol);
        self.market_positions
            .lock()
            .map_err(|_| BinanceExecutionError::Unavailable)?
            .insert(key, position);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn market_delta_cannot_cross_zero_and_supports_partial_terminal_fills() {
        assert_eq!(
            expected_market_position(Decimal::ONE, Decimal::new(3, 1), true),
            Some(Decimal::new(7, 1))
        );
        assert_eq!(
            expected_market_position(Decimal::ONE, Decimal::from(2), true),
            None
        );
        assert_eq!(
            expected_market_position(Decimal::ZERO, Decimal::ONE, false),
            Some(Decimal::ONE)
        );
    }
}
