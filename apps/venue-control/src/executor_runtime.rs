//! One-shot and long-running orchestration for the singleton Binance writer.
//!
//! The runtime intentionally has no retry queue. PostgreSQL is the queue and a command that has
//! crossed the physical-send boundary can only be read back with its original client order ID.

use std::time::{SystemTime, UNIX_EPOCH};

use venue_control_protocol::kol::ExecutorCommandState;
use venue_gateway_binance::BinanceCredentials;

use crate::{
    executor_exchange::{
        AccountBaseline, BinanceActivationBaseline, BinanceExecution, ExecutionOrderKind,
        ExecutionReadback, ExecutionRequest,
    },
    executor_secret::{ExecutorSecretError, ExecutorSecretProvider},
    executor_store::PgExecutorStore,
    kol_executor::{BinanceCommandLedgerError, ClaimedBinanceCommand},
};

#[allow(
    async_fn_in_trait,
    reason = "the executor owns this narrow credential boundary and has no third-party implementors"
)]
pub trait ExecutorCredentials {
    async fn credentials(
        &self,
        credential_id: &str,
        owner_user_id: &str,
    ) -> Result<BinanceCredentials, ExecutorSecretError>;
}

impl ExecutorCredentials for ExecutorSecretProvider {
    async fn credentials(
        &self,
        credential_id: &str,
        owner_user_id: &str,
    ) -> Result<BinanceCredentials, ExecutorSecretError> {
        self.load(credential_id, owner_user_id).await
    }
}

/// Drives durable commands through exactly one submit or an arbitrary number of exact readbacks.
/// `E` is deliberately injected so offline fixtures have the identical transition semantics as a
/// production Binance adapter without allowing a fixture endpoint in production configuration.
pub struct BinanceExecutorRuntime<E, S> {
    store: PgExecutorStore,
    exchange: E,
    secrets: S,
}

impl<E, S> BinanceExecutorRuntime<E, S>
where
    E: BinanceExecution + BinanceActivationBaseline,
    S: ExecutorCredentials,
{
    #[must_use]
    pub fn new(store: PgExecutorStore, exchange: E, secrets: S) -> Self {
        Self {
            store,
            exchange,
            secrets,
        }
    }

    /// Reconciles all already-sent work before considering the oldest Pending command for each
    /// account. A database restart therefore cannot turn uncertainty into a second POST.
    pub async fn recover_once(&mut self) -> Result<usize, BinanceCommandLedgerError> {
        self.process_pending_activations().await?;
        let commands = self.store.recover_nonterminal().await?;
        let mut processed = 0_usize;
        for command in &commands {
            if command.state != ExecutorCommandState::Pending {
                self.reconcile(command).await?;
                processed = processed.saturating_add(1);
            }
        }
        let mut accounts = std::collections::BTreeSet::new();
        for command in commands {
            accounts.insert(command.trading_account_id);
        }
        for account in accounts {
            if let Some(command) = self.store.claim_next_command(&account, now_ms()?).await? {
                self.submit(command).await?;
                processed = processed.saturating_add(1);
            }
        }
        Ok(processed)
    }

    async fn process_pending_activations(&mut self) -> Result<(), BinanceCommandLedgerError> {
        for activation in self.store.pending_activations(now_ms()?).await? {
            let leader = self
                .secrets
                .credentials(&activation.leader_credential_id, &activation.leader_user_id)
                .await;
            let follower = self
                .secrets
                .credentials(
                    &activation.follower_credential_id,
                    &activation.follower_user_id,
                )
                .await;
            let clean = match (leader, follower) {
                (Ok(leader), Ok(follower)) => {
                    matches!(
                        self.exchange
                            .activation_baseline(
                                &activation.leader_trading_account_id,
                                &activation.symbols,
                                leader,
                            )
                            .await,
                        Ok(AccountBaseline::Clean)
                    ) && matches!(
                        self.exchange
                            .activation_baseline(
                                &activation.follower_trading_account_id,
                                &activation.symbols,
                                follower,
                            )
                            .await,
                        Ok(AccountBaseline::Clean)
                    )
                }
                _ => false,
            };
            if clean {
                self.store
                    .complete_activation(&activation.relation_id, activation.revision, now_ms()?)
                    .await?;
            } else {
                self.store
                    .reject_activation(&activation.relation_id, now_ms()?, "baseline_failed")
                    .await?;
            }
        }
        Ok(())
    }

    /// A small bounded polling loop for the process entrypoint. It has no in-memory command
    /// authority: stopping it merely leaves rows for the next singleton to recover.
    pub async fn run_until_shutdown(
        &mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), BinanceCommandLedgerError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let _ = self.recover_once().await?;
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            }
        }
    }

    async fn submit(
        &mut self,
        command: ClaimedBinanceCommand,
    ) -> Result<(), BinanceCommandLedgerError> {
        let credentials = match self
            .secrets
            .credentials(&command.credential_id, &command.owner_user_id)
            .await
        {
            Ok(credentials) => credentials,
            // No request was made when credential retrieval failed, so rejection is safe and
            // does not mislabel a possibly accepted Binance mutation as a rejection.
            Err(_) => {
                return self
                    .store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Rejected,
                        now_ms()?,
                        Some("credential_unavailable"),
                    )
                    .await;
            }
        };
        let request = request(&command);
        match self.exchange.submit(&request, credentials).await {
            Ok(ExecutionReadback::Rejected) => {
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Rejected,
                        now_ms()?,
                        Some("binance_rejected"),
                    )
                    .await
            }
            Ok(ExecutionReadback::Unknown) | Err(_) => {
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::ReconcileRequired,
                        now_ms()?,
                        Some("dispatch_unknown"),
                    )
                    .await
            }
            Ok(ExecutionReadback::Accepted) => {
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Accepted,
                        now_ms()?,
                        None,
                    )
                    .await?;
                self.readback_after_accepted(&command).await
            }
            Ok(ExecutionReadback::Reconciled) => {
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Accepted,
                        now_ms()?,
                        None,
                    )
                    .await?;
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Reconciled,
                        now_ms()?,
                        None,
                    )
                    .await
            }
        }
    }

    async fn reconcile(
        &mut self,
        command: &ClaimedBinanceCommand,
    ) -> Result<(), BinanceCommandLedgerError> {
        if command.state == ExecutorCommandState::Sending {
            self.store
                .transition_command(
                    &command.command_id,
                    ExecutorCommandState::ReconcileRequired,
                    now_ms()?,
                    Some("restart_reconcile"),
                )
                .await?;
        }
        let credentials = match self
            .secrets
            .credentials(&command.credential_id, &command.owner_user_id)
            .await
        {
            Ok(credentials) => credentials,
            // Retain the durable uncertainty fence. A missing key is not evidence of absence.
            Err(_) => return Ok(()),
        };
        self.readback_with_credentials(command, credentials).await
    }

    async fn readback_after_accepted(
        &mut self,
        command: &ClaimedBinanceCommand,
    ) -> Result<(), BinanceCommandLedgerError> {
        let credentials = match self
            .secrets
            .credentials(&command.credential_id, &command.owner_user_id)
            .await
        {
            Ok(credentials) => credentials,
            Err(_) => return Ok(()),
        };
        self.readback_with_credentials(command, credentials).await
    }

    async fn readback_with_credentials(
        &mut self,
        command: &ClaimedBinanceCommand,
        credentials: BinanceCredentials,
    ) -> Result<(), BinanceCommandLedgerError> {
        match self.exchange.readback(&request(command), credentials).await {
            Ok(ExecutionReadback::Reconciled) => {
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Reconciled,
                        now_ms()?,
                        None,
                    )
                    .await
            }
            Ok(ExecutionReadback::Rejected)
            | Ok(ExecutionReadback::Accepted)
            | Ok(ExecutionReadback::Unknown)
            | Err(_) => Ok(()),
        }
    }
}

fn request(command: &ClaimedBinanceCommand) -> ExecutionRequest {
    ExecutionRequest {
        command_id: command.command_id.clone(),
        client_order_id: command.client_order_id.clone(),
        trading_account_id: command.trading_account_id.clone(),
        symbol: command.symbol.clone(),
        side: command.side,
        position_side: command.position_side,
        quantity: command.quantity,
        order_kind: match command.order_kind {
            venue_control_protocol::kol::TerminalOrderKind::Market => ExecutionOrderKind::Market,
            venue_control_protocol::kol::TerminalOrderKind::LimitPostOnly => {
                ExecutionOrderKind::LimitPostOnly {
                    price: command.limit_price.unwrap_or(rust_decimal::Decimal::ZERO),
                }
            }
        },
        reducing: command.reducing,
    }
}

fn now_ms() -> Result<u64, BinanceCommandLedgerError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceCommandLedgerError::Unavailable)?
        .as_millis();
    u64::try_from(value).map_err(|_| BinanceCommandLedgerError::Unavailable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_keeps_the_durable_identities_verbatim() -> Result<(), Box<dyn std::error::Error>> {
        let command = ClaimedBinanceCommand {
            command_id: "command".into(),
            owner_user_id: "owner".into(),
            trading_account_id: "account".into(),
            credential_id: "credential".into(),
            symbol: "BTC/USDT".parse()?,
            side: venue_domain::domain::OrderSide::Buy,
            position_side: venue_domain::domain::PositionSide::Long,
            quantity: rust_decimal::Decimal::new(1, 3),
            order_kind: venue_control_protocol::kol::TerminalOrderKind::Market,
            limit_price: None,
            reducing: false,
            client_order_id: "client".into(),
            state: ExecutorCommandState::ReconcileRequired,
        };
        assert_eq!(
            request(&command),
            ExecutionRequest {
                command_id: "command".into(),
                client_order_id: "client".into(),
                trading_account_id: "account".into(),
                symbol: "BTC/USDT".parse()?,
                side: venue_domain::domain::OrderSide::Buy,
                position_side: venue_domain::domain::PositionSide::Long,
                quantity: rust_decimal::Decimal::new(1, 3),
                order_kind: ExecutionOrderKind::Market,
                reducing: false,
            }
        );
        Ok(())
    }
}
