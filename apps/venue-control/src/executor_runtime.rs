//! The executor's narrow state-convergence loop.
//!
//! A command has entered `sending` before this module receives it. Therefore no failure in this
//! module can return it to `pending`: a missing credential, malformed response, timeout, or
//! disconnected read is always held for exact-client-id reconciliation.

use std::collections::BTreeSet;

use venue_control_protocol::kol::ExecutorCommandState;

use crate::{
    executor_exchange::{BinanceExecution, ExecutionReadback, ExecutionRequest},
    executor_secret::ExecutorSecretProvider,
    executor_store::PgExecutorStore,
    kol_executor::{BinanceCommandLedgerError, ClaimedBinanceCommand},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutorRunReport {
    pub submitted: usize,
    pub read_back: usize,
    pub reconciled: usize,
    pub rejected: usize,
    pub reconcile_required: usize,
}

/// Runs one bounded sweep. The caller owns cadence and cancellation; this type never creates an
/// account task, writer lease, or local recovery file.
pub struct BinanceExecutorRuntime<E> {
    store: PgExecutorStore,
    secrets: ExecutorSecretProvider,
    exchange: E,
}

impl<E> BinanceExecutorRuntime<E>
where
    E: BinanceExecution,
{
    #[must_use]
    pub fn new(store: PgExecutorStore, secrets: ExecutorSecretProvider, exchange: E) -> Self {
        Self {
            store,
            secrets,
            exchange,
        }
    }

    pub fn exchange_mut(&mut self) -> &mut E {
        &mut self.exchange
    }

    /// First reconciles work that may already have reached Binance, then claims at most one new
    /// command per discovered account. Repeated calls drain later commands without violating the
    /// PostgreSQL Sending/ReconcileRequired account fence.
    pub async fn sweep(
        &mut self,
        now_ms: u64,
    ) -> Result<ExecutorRunReport, BinanceCommandLedgerError> {
        let mut report = ExecutorRunReport::default();
        let commands = self.store.recover_nonterminal().await?;
        let accounts = commands
            .iter()
            .map(|command| command.trading_account_id.clone())
            .collect::<BTreeSet<_>>();

        for command in commands
            .into_iter()
            .filter(|command| command.state != ExecutorCommandState::Pending)
        {
            self.reconcile(command, now_ms, &mut report).await?;
        }
        for account in accounts {
            if let Some(command) = self.store.claim_next_command(&account, now_ms).await? {
                self.submit(command, now_ms, &mut report).await?;
            }
        }
        Ok(report)
    }

    async fn submit(
        &mut self,
        command: ClaimedBinanceCommand,
        now_ms: u64,
        report: &mut ExecutorRunReport,
    ) -> Result<(), BinanceCommandLedgerError> {
        let credentials = match self
            .secrets
            .load(&command.credential_id, &command.owner_user_id)
            .await
        {
            Ok(credentials) => credentials,
            Err(_) => return self.require_reconciliation(&command, now_ms, report).await,
        };
        let request = request(&command);
        report.submitted += 1;
        match self.exchange.submit(&request, credentials).await {
            Ok(ExecutionReadback::Accepted | ExecutionReadback::PartiallyFilled) => {
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Accepted,
                        now_ms,
                        None,
                    )
                    .await?;
                // An ACK is not a completed order. A later sweep performs the independent exact
                // readback, keeping this physical POST singular even if the process exits here.
            }
            Ok(ExecutionReadback::Reconciled) => {
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Accepted,
                        now_ms,
                        None,
                    )
                    .await?;
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Reconciled,
                        now_ms,
                        None,
                    )
                    .await?;
                report.reconciled += 1;
            }
            Ok(ExecutionReadback::Rejected) => {
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Rejected,
                        now_ms,
                        Some("exchange_rejected"),
                    )
                    .await?;
                report.rejected += 1;
            }
            Ok(ExecutionReadback::Unknown) | Err(_) => {
                self.require_reconciliation(&command, now_ms, report)
                    .await?
            }
        }
        Ok(())
    }

    async fn reconcile(
        &mut self,
        command: ClaimedBinanceCommand,
        now_ms: u64,
        report: &mut ExecutorRunReport,
    ) -> Result<(), BinanceCommandLedgerError> {
        let credentials = match self
            .secrets
            .load(&command.credential_id, &command.owner_user_id)
            .await
        {
            Ok(credentials) => credentials,
            Err(_) => return self.require_reconciliation(&command, now_ms, report).await,
        };
        report.read_back += 1;
        match self
            .exchange
            .readback(&request(&command), credentials)
            .await
        {
            Ok(ExecutionReadback::Reconciled) => {
                if command.state == ExecutorCommandState::Sending {
                    self.store
                        .transition_command(
                            &command.command_id,
                            ExecutorCommandState::ReconcileRequired,
                            now_ms,
                            None,
                        )
                        .await?;
                }
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Reconciled,
                        now_ms,
                        None,
                    )
                    .await?;
                report.reconciled += 1;
            }
            Ok(ExecutionReadback::Rejected) => {
                self.store
                    .transition_command(
                        &command.command_id,
                        ExecutorCommandState::Rejected,
                        now_ms,
                        Some("exchange_rejected"),
                    )
                    .await?;
                report.rejected += 1;
            }
            Ok(ExecutionReadback::Accepted | ExecutionReadback::PartiallyFilled) => {
                if command.state == ExecutorCommandState::Sending {
                    self.store
                        .transition_command(
                            &command.command_id,
                            ExecutorCommandState::Accepted,
                            now_ms,
                            None,
                        )
                        .await?;
                }
            }
            Ok(ExecutionReadback::Unknown) | Err(_) => {
                self.require_reconciliation(&command, now_ms, report)
                    .await?
            }
        }
        Ok(())
    }

    async fn require_reconciliation(
        &self,
        command: &ClaimedBinanceCommand,
        now_ms: u64,
        report: &mut ExecutorRunReport,
    ) -> Result<(), BinanceCommandLedgerError> {
        if command.state != ExecutorCommandState::ReconcileRequired {
            self.store
                .transition_command(
                    &command.command_id,
                    ExecutorCommandState::ReconcileRequired,
                    now_ms,
                    Some("reconcile_required"),
                )
                .await?;
            report.reconcile_required += 1;
        }
        Ok(())
    }
}

fn request(command: &ClaimedBinanceCommand) -> ExecutionRequest {
    ExecutionRequest {
        command_id: command.command_id.clone(),
        client_order_id: command.client_order_id.clone(),
        trading_account_id: command.trading_account_id.clone(),
    }
}
