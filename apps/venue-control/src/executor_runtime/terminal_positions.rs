use super::*;
use crate::executor_exchange::TerminalMarketResult;

pub(super) async fn submit_position<E: BinanceExecution + Send>(
    store: &PgExecutorStore,
    exchange: &mut E,
    command: &ClaimedBinanceCommand,
    credentials: BinanceCredentials,
) -> Result<AccountDrainDecision, BinanceCommandLedgerError> {
    let context = match store.position_context(command, false, now_ms()?).await {
        Ok(context) => context,
        Err(error) => {
            let code = match error {
                BinanceCommandLedgerError::Conflict => "position_changed_or_unavailable",
                _ => "not_dispatched_unavailable",
            };
            store
                .transition_command(
                    &command.command_id,
                    ExecutorCommandState::Rejected,
                    now_ms()?,
                    Some(code),
                )
                .await?;
            return Ok(AccountDrainDecision::Continue);
        }
    };
    let result = exchange
        .terminal_market(&request(command), &context, credentials, false)
        .await;
    match result {
        Ok(result) => {
            save_settlement(store, command, &result).await?;
            if let Some(code) = result.failure_code {
                store
                    .transition_command_with_readback(
                        &command.command_id,
                        ExecutorCommandState::Rejected,
                        now_ms()?,
                        Some(code),
                        result.outcome.native_order_id.as_deref(),
                    )
                    .await?;
                Ok(AccountDrainDecision::Continue)
            } else {
                settle_submit_result(store, command, result.outcome).await
            }
        }
        Err(error) => {
            let (state, code) = not_dispatched_transition(error);
            store
                .transition_command(&command.command_id, state, now_ms()?, Some(code))
                .await?;
            Ok(drain_after_persisted_state(state))
        }
    }
}

pub(super) async fn readback_position<E: BinanceExecution + Send>(
    store: &PgExecutorStore,
    exchange: &mut E,
    command: &RecoverableBinanceCommand,
    credentials: BinanceCredentials,
) -> Result<ExecutionOutcome, crate::executor_exchange::BinanceExecutionError> {
    let context = store
        .position_context(
            command,
            true,
            now_ms().map_err(|_| crate::executor_exchange::BinanceExecutionError::Unavailable)?,
        )
        .await
        .map_err(|_| crate::executor_exchange::BinanceExecutionError::Unavailable)?;
    let result = exchange
        .terminal_market(&request(command), &context, credentials, true)
        .await?;
    save_settlement(store, command, &result)
        .await
        .map_err(|_| crate::executor_exchange::BinanceExecutionError::Unavailable)?;
    Ok(result.outcome)
}

async fn save_settlement(
    store: &PgExecutorStore,
    command: &ClaimedBinanceCommand,
    result: &TerminalMarketResult,
) -> Result<(), BinanceCommandLedgerError> {
    if let Some(settlement) = &result.settlement {
        store.save_position_settlement(command, settlement).await?;
    }
    Ok(())
}
