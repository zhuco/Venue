use super::*;
use crate::executor_exchange::{ExactOrderFact, ExecutionOutcome, ExecutionReadback};

fn remaining(fact: &ExactOrderFact, original: Decimal) -> Option<Decimal> {
    (fact.terminal
        && fact.quantity == original
        && fact.filled_quantity >= Decimal::ZERO
        && fact.filled_quantity <= original)
        .then(|| original - fact.filled_quantity)
}

impl PgExecutorStore {
    pub(crate) async fn record_replace_fact(
        &self,
        command: &ClaimedBinanceCommand,
        result: &ExecutionOutcome,
    ) -> Result<(), BinanceCommandLedgerError> {
        if command.origin != venue_control_protocol::kol::ExecutorCommandOrigin::Terminal
            || !matches!(command.order, ClaimedBinanceOrder::CancelExact { .. })
        {
            return Ok(());
        }
        let original: Option<String> = sqlx::query_scalar(
            "SELECT original_quantity FROM venue_terminal_replacements WHERE cancel_command_id=$1",
        )
        .bind(&command.command_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let Some(original) = original else {
            return Ok(());
        };
        let original =
            Decimal::from_str(&original).map_err(|_| BinanceCommandLedgerError::Conflict)?;
        let exact_identity = match &command.order {
            ClaimedBinanceOrder::CancelExact {
                native_order_id: Some(id),
                ..
            } => result.native_order_id.as_ref() == Some(id),
            _ => false,
        };
        let quantity = result
            .order_fact
            .as_ref()
            .filter(|_| exact_identity && result.state == ExecutionReadback::Reconciled)
            .and_then(|fact| remaining(fact, original));
        sqlx::query("UPDATE venue_terminal_replacements SET remaining_quantity=$2 WHERE cancel_command_id=$1 AND NOT released")
            .bind(&command.command_id).bind(quantity.map(|q| q.normalize().to_string())).execute(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        Ok(())
    }
}

pub(crate) async fn settle_replace_child(
    connection: &mut PgConnection,
    parent: &str,
    state: ExecutorCommandState,
    now: i64,
) -> Result<(), BinanceCommandLedgerError> {
    if !matches!(
        state,
        ExecutorCommandState::Reconciled
            | ExecutorCommandState::Rejected
            | ExecutorCommandState::Cancelled
    ) {
        return Ok(());
    }
    let row = sqlx::query("SELECT command_id,remaining_quantity FROM venue_terminal_replacements WHERE cancel_command_id=$1 AND NOT released FOR UPDATE")
        .bind(parent).fetch_optional(&mut *connection).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let Some(row) = row else {
        return Ok(());
    };
    let id: String = row
        .try_get("command_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let quantity: Option<String> = row
        .try_get("remaining_quantity")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let quantity = quantity
        .map(|q| Decimal::from_str(&q))
        .transpose()
        .map_err(|_| BinanceCommandLedgerError::Conflict)?;
    if let Some(quantity) =
        quantity.filter(|q| state == ExecutorCommandState::Reconciled && *q > Decimal::ZERO)
    {
        sqlx::query("UPDATE venue_binance_commands SET requested_quantity=$2,updated_ms=$3 WHERE command_id=$1 AND command_state='pending'")
            .bind(&id).bind(quantity.normalize().to_string()).bind(now).execute(&mut *connection).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        sqlx::query("UPDATE venue_terminal_replacements SET released=true WHERE command_id=$1")
            .bind(id)
            .execute(connection)
            .await
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    } else {
        sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$2,updated_ms=$2,sanitized_error_code='replacement_not_released' WHERE command_id=$1 AND command_state='pending'")
            .bind(id).bind(now).execute(connection).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replacement_uses_final_unfilled_quantity_only() {
        let mut fact = ExactOrderFact {
            quantity: Decimal::from(10),
            filled_quantity: Decimal::from(4),
            terminal: true,
        };
        assert_eq!(remaining(&fact, Decimal::from(10)), Some(Decimal::from(6)));
        fact.terminal = false;
        assert_eq!(remaining(&fact, Decimal::from(10)), None);
        fact.terminal = true;
        fact.filled_quantity = Decimal::from(10);
        assert_eq!(remaining(&fact, Decimal::from(10)), Some(Decimal::ZERO));
        fact.filled_quantity = Decimal::from(11);
        assert_eq!(remaining(&fact, Decimal::from(10)), None);
        assert_eq!(remaining(&fact, Decimal::from(12)), None);
    }
}
