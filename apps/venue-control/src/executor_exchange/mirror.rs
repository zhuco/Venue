use super::*;

impl BinanceHttpExecution {
    pub(super) async fn read_mirror_cancel_fact(
        &mut self,
        request: &ExecutionRequest,
        native: Option<&str>,
        client: Option<&str>,
        credentials: &BinanceCredentials,
        scope: &BinancePrivateReadScope,
    ) -> Result<ExecutionOutcome, BinanceExecutionError> {
        let client = client.ok_or(BinanceExecutionError::Invalid)?;
        match self
            .exact_order_for_client_in_scope(request, credentials, client, scope)
            .await
        {
            Ok(order) if native == Some(order.order_id.as_str()) => {
                Ok(mirror_order_outcome(&order, true))
            }
            _ => Ok(outcome(
                ExecutionReadback::Unknown,
                native.map(str::to_owned),
            )),
        }
    }
}

pub(super) fn mirror_order_outcome(
    order: &venue_domain::domain::Order,
    cancel: bool,
) -> ExecutionOutcome {
    if order.state == OrderState::Unknown
        || order.quantity <= Decimal::ZERO
        || order.filled_quantity < Decimal::ZERO
        || order.filled_quantity > order.quantity
    {
        return outcome(ExecutionReadback::Unknown, Some(order.order_id.clone()));
    }
    let terminal = terminal_order_state(order.state);
    let state = if order.state == OrderState::Rejected && !cancel {
        ExecutionReadback::Rejected
    } else if terminal || !cancel {
        ExecutionReadback::Reconciled
    } else {
        ExecutionReadback::Accepted
    };
    let mut result = outcome(state, Some(order.order_id.clone()));
    result.order_fact = Some(ExactOrderFact {
        quantity: order.quantity,
        filled_quantity: order.filled_quantity,
        terminal,
    });
    result
}
