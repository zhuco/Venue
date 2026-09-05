use super::*;
use crate::executor_exchange::{TerminalMarketContext, TerminalPositionSettlement};
use venue_domain::PositionSide;

impl PgExecutorStore {
    pub(crate) async fn position_was_prepared(
        &self,
        id: &str,
    ) -> Result<bool, BinanceCommandLedgerError> {
        sqlx::query_scalar("SELECT prepared_json IS NOT NULL FROM venue_terminal_position_commands WHERE command_id=$1")
            .bind(id).fetch_one(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)
    }
    pub(crate) async fn is_position_command(
        &self,
        id: &str,
    ) -> Result<bool, BinanceCommandLedgerError> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM venue_terminal_position_commands WHERE command_id=$1)",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)
    }

    pub(crate) async fn position_context(
        &self,
        command: &ClaimedBinanceCommand,
        read_only: bool,
        now: u64,
    ) -> Result<TerminalMarketContext, BinanceCommandLedgerError> {
        let row = sqlx::query("SELECT prepared_json,reverse_parent_id,released FROM venue_terminal_position_commands WHERE command_id=$1")
            .bind(&command.command_id).fetch_one(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let prepared: Option<serde_json::Value> = row
            .try_get("prepared_json")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        if read_only {
            return serde_json::from_value(prepared.ok_or(BinanceCommandLedgerError::Conflict)?)
                .map_err(|_| BinanceCommandLedgerError::Conflict);
        }
        if prepared.is_some() || !self.terminal_open_credential_verified(command).await? {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let projection =
            crate::private_projection::BinancePrivateProjectionStore::new(self.pool.clone())
                .load_healthy_owned(&command.owner_user_id, &command.credential_id)
                .await
                .map_err(|_| BinanceCommandLedgerError::Unavailable)?
                .ok_or(BinanceCommandLedgerError::Conflict)?;
        if projection.trading_account_id != command.trading_account_id
            || projection.observed_ms > now
            || now.saturating_sub(projection.observed_ms) > 15_000
        {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let ClaimedBinanceOrder::Market {
            position_side,
            quantity,
            reducing,
            ..
        } = command.order
        else {
            return Err(BinanceCommandLedgerError::Conflict);
        };
        let parent: Option<String> = row
            .try_get("reverse_parent_id")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let released: bool = row
            .try_get("released")
            .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        let quantity = if reducing {
            projection
                .positions
                .iter()
                .find(|p| p.symbol == command.symbol && p.position_side == position_side)
                .map(|p| p.quantity.min(quantity))
                .filter(|q| *q > Decimal::ZERO)
                .ok_or(BinanceCommandLedgerError::Conflict)?
        } else {
            if parent.is_none() || !released {
                return Err(BinanceCommandLedgerError::Conflict);
            }
            let original_side = match position_side {
                PositionSide::Long => PositionSide::Short,
                PositionSide::Short => PositionSide::Long,
                PositionSide::Net => return Err(BinanceCommandLedgerError::Conflict),
            };
            if !projection.positions.iter().any(|p| {
                p.symbol == command.symbol
                    && p.position_side == original_side
                    && p.quantity.is_zero()
            }) {
                return Err(BinanceCommandLedgerError::Conflict);
            }
            quantity
        };
        let context = TerminalMarketContext {
            quantity,
            private_generation: projection.private_generation,
            observed_ms: projection.observed_ms,
        };
        let changed = sqlx::query("UPDATE venue_terminal_position_commands SET prepared_json=$2 WHERE command_id=$1 AND prepared_json IS NULL AND released")
            .bind(&command.command_id).bind(serde_json::to_value(&context).map_err(|_| BinanceCommandLedgerError::Conflict)?)
            .execute(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        if changed.rows_affected() != 1 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        Ok(context)
    }

    pub(crate) async fn save_position_settlement(
        &self,
        command: &ClaimedBinanceCommand,
        settlement: &TerminalPositionSettlement,
    ) -> Result<(), BinanceCommandLedgerError> {
        if settlement.observed_ms == 0
            || settlement.positions.len() != 2
            || ![PositionSide::Long, PositionSide::Short]
                .iter()
                .all(|side| {
                    settlement.positions.iter().any(|p| {
                        p.symbol == command.symbol
                            && p.position_side == *side
                            && p.quantity >= Decimal::ZERO
                    })
                })
            || settlement.executed_quantity < Decimal::ZERO
        {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        sqlx::query("UPDATE venue_terminal_position_commands SET settlement_json=$2 WHERE command_id=$1 AND prepared_json IS NOT NULL")
            .bind(&command.command_id).bind(serde_json::to_value(settlement).map_err(|_| BinanceCommandLedgerError::Conflict)?)
            .execute(&self.pool).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        Ok(())
    }
}

/// Runs inside the parent's ledger transition: a failed/unknown close can never release its open.
pub(crate) async fn settle_reverse_child(
    connection: &mut PgConnection,
    parent_id: &str,
    state: ExecutorCommandState,
    now: i64,
    reason: Option<&str>,
) -> Result<(), BinanceCommandLedgerError> {
    if !matches!(
        state,
        ExecutorCommandState::Reconciled
            | ExecutorCommandState::Rejected
            | ExecutorCommandState::Cancelled
    ) {
        return Ok(());
    }
    let row = sqlx::query("SELECT child.command_id,parent.position_side,meta.settlement_json FROM venue_terminal_position_commands child JOIN venue_binance_commands parent ON parent.command_id=child.reverse_parent_id JOIN venue_terminal_position_commands meta ON meta.command_id=parent.command_id WHERE child.reverse_parent_id=$1 AND NOT child.released")
        .bind(parent_id).fetch_optional(&mut *connection).await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let Some(row) = row else {
        return Ok(());
    };
    let child_id: String = row
        .try_get("command_id")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let side: String = row
        .try_get("position_side")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let payload: Option<serde_json::Value> = row
        .try_get("settlement_json")
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    let settlement = payload
        .map(serde_json::from_value::<TerminalPositionSettlement>)
        .transpose()
        .map_err(|_| BinanceCommandLedgerError::Conflict)?;
    let filled = settlement
        .as_ref()
        .filter(|s| {
            state == ExecutorCommandState::Reconciled
                && s.executed_quantity > Decimal::ZERO
                && s.positions.iter().any(|p| {
                    ((side == "long" && p.position_side == PositionSide::Long)
                        || (side == "short" && p.position_side == PositionSide::Short))
                        && p.quantity.is_zero()
                })
        })
        .map(|s| s.executed_quantity);
    if let Some(quantity) = filled {
        sqlx::query("UPDATE venue_binance_commands SET requested_quantity=$2,updated_ms=$3 WHERE command_id=$1 AND command_state='pending'")
            .bind(&child_id).bind(quantity.normalize().to_string()).bind(now).execute(&mut *connection)
            .await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
        sqlx::query(
            "UPDATE venue_terminal_position_commands SET released=true WHERE command_id=$1",
        )
        .bind(child_id)
        .execute(&mut *connection)
        .await
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    } else {
        sqlx::query("UPDATE venue_binance_commands SET command_state='cancelled',terminal_ms=$2,updated_ms=$2,sanitized_error_code=$3 WHERE command_id=$1 AND command_state='pending'")
            .bind(child_id).bind(now).bind(reason.unwrap_or("reverse_position_not_flat")).execute(connection)
            .await.map_err(|_| BinanceCommandLedgerError::Unavailable)?;
    }
    Ok(())
}
