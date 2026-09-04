//! One-shot and long-running orchestration for the singleton Binance writer.
//!
//! The runtime intentionally has no retry queue. PostgreSQL is the queue and a command that has
//! crossed the physical-send boundary can only be read back with its original client order ID.

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use venue_control_protocol::kol::ExecutorCommandState;
use venue_gateway_binance::BinanceCredentials;

use crate::{
    executor_exchange::{
        AccountBaseline, BinanceActivationBaseline, BinanceExecution, ExecutionOrderKind,
        ExecutionOutcome, ExecutionReadback, ExecutionRequest, GridBatchCommandOutcome,
        GridBatchExecutionContext, GridBatchSubmitError,
    },
    executor_secret::{ExecutorSecretError, ExecutorSecretProvider},
    executor_store::{PgExecutorStore, RecoverableBinanceCommand},
    kol_executor::{
        AccountSerialScheduler, BinanceCommandLedgerError, ClaimedBinanceBatch,
        ClaimedBinanceCommand, ClaimedBinanceOrder, MAX_ACCOUNT_QUEUE_DEPTH, MAX_GLOBAL_IN_FLIGHT,
    },
};

const EXECUTOR_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const ACTIVATION_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
const ACCOUNT_DRAIN_QUANTUM: usize = MAX_ACCOUNT_QUEUE_DEPTH;

enum ActivationTurn<T> {
    Stop,
    Retry,
    CommandWake,
    Completed(T),
}

/// Process-local latency hint for newly committed PostgreSQL commands. PostgreSQL remains the
/// queue and recovery authority: notifications may be coalesced or lost with the process, while
/// the bounded poll still discovers every durable row after restart.
#[derive(Clone, Debug, Default)]
pub struct CommandWake {
    notify: Arc<tokio::sync::Notify>,
}

impl CommandWake {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Wakes one executor discovery turn. `Notify::notify_one` retains one permit when the
    /// executor is between turns, so a commit immediately before the wait is not delayed by the
    /// polling fallback.
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    async fn notified(&self) {
        self.notify.notified().await;
    }
}

pub trait ExecutorCredentials {
    fn credentials<'a>(
        &'a self,
        credential_id: &'a str,
        owner_user_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<BinanceCredentials, ExecutorSecretError>> + Send + 'a>>;
}

impl ExecutorCredentials for ExecutorSecretProvider {
    fn credentials<'a>(
        &'a self,
        credential_id: &'a str,
        owner_user_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<BinanceCredentials, ExecutorSecretError>> + Send + 'a>>
    {
        Box::pin(self.load(credential_id, owner_user_id))
    }
}

/// Drives durable commands through exactly one submit or an arbitrary number of exact readbacks.
/// `E` is deliberately injected so offline fixtures have the identical transition semantics as a
/// production Binance adapter without allowing a fixture endpoint in production configuration.
pub struct BinanceExecutorRuntime<E, S> {
    store: PgExecutorStore,
    exchange: E,
    secrets: Arc<S>,
    command_wake: CommandWake,
}

enum ScheduledCommand {
    Claim,
    Claimed(ClaimedBinanceBatch),
    Reconcile(Vec<RecoverableBinanceCommand>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AccountDrainDecision {
    Continue,
    Stop,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AccountDrainResult {
    processed: usize,
    quantum_exhausted: bool,
}

const fn drain_after_persisted_state(state: ExecutorCommandState) -> AccountDrainDecision {
    match state {
        ExecutorCommandState::Rejected
        | ExecutorCommandState::Reconciled
        | ExecutorCommandState::Cancelled => AccountDrainDecision::Continue,
        ExecutorCommandState::Pending
        | ExecutorCommandState::Sending
        | ExecutorCommandState::Accepted
        | ExecutorCommandState::ReconcileRequired => AccountDrainDecision::Stop,
    }
}

fn continue_account_drain(processed: usize, decision: AccountDrainDecision) -> bool {
    decision == AccountDrainDecision::Continue && processed < ACCOUNT_DRAIN_QUANTUM
}

impl<E, S> BinanceExecutorRuntime<E, S>
where
    E: BinanceExecution + BinanceActivationBaseline + Clone + Send + 'static,
    S: ExecutorCredentials + Send + Sync + 'static,
{
    #[must_use]
    pub fn new(store: PgExecutorStore, exchange: E, secrets: S) -> Self {
        Self::with_command_wake(store, exchange, secrets, CommandWake::new())
    }

    #[must_use]
    pub fn with_command_wake(
        store: PgExecutorStore,
        exchange: E,
        secrets: S,
        command_wake: CommandWake,
    ) -> Self {
        Self {
            store,
            exchange,
            secrets: Arc::new(secrets),
            command_wake,
        }
    }

    /// A producer calls this handle only after its command transaction commits. It carries no
    /// order data or authority and is safe to clone across the singleton process.
    #[must_use]
    pub fn command_wake(&self) -> CommandWake {
        self.command_wake.clone()
    }

    /// Reconciles all already-sent work before considering the oldest Pending command for each
    /// account. A database restart therefore cannot turn uncertainty into a second POST.
    pub async fn recover_once(&mut self) -> Result<usize, BinanceCommandLedgerError> {
        self.process_pending_activations().await?;
        self.recover_commands_once().await
    }

    async fn recover_commands_once(&mut self) -> Result<usize, BinanceCommandLedgerError> {
        let commands = self.store.recover_nonterminal().await?;
        let recovery_ms = now_ms()?;
        let accounts = account_recovery_groups(commands);
        let mut scheduler = AccountSerialScheduler::new(MAX_GLOBAL_IN_FLIGHT);
        for (account, unresolved) in accounts {
            match unresolved {
                Some(commands) => {
                    let mut any_due = false;
                    for command in &commands {
                        any_due |= command.reconciliation_due(recovery_ms)?;
                    }
                    if any_due {
                        scheduler.enqueue(account, ScheduledCommand::Reconcile(commands))?;
                    }
                    // The durable row still fences this account. Its signed readback is simply
                    // not due during this discovery tick.
                }
                None => scheduler.enqueue(account, ScheduledCommand::Claim)?,
            }
        }
        let mut tasks = tokio::task::JoinSet::new();
        let mut processed = 0_usize;
        loop {
            while let Some(queued) = scheduler.claim_next() {
                let store = self.store.clone();
                let exchange = self.exchange.clone();
                let secrets = Arc::clone(&self.secrets);
                tasks.spawn(async move {
                    let account = queued.trading_account_id;
                    let result =
                        execute_scheduled(store, exchange, secrets, &account, queued.command)
                            .await?;
                    Ok::<_, BinanceCommandLedgerError>((account, result))
                });
            }
            if tasks.is_empty() {
                break;
            }
            let joined = tasks
                .join_next()
                .await
                .ok_or(BinanceCommandLedgerError::Unavailable)?
                .map_err(|_| BinanceCommandLedgerError::Unavailable)??;
            scheduler.settle(&joined.0)?;
            processed = processed.saturating_add(joined.1.processed);
            if joined.1.quantum_exhausted {
                // One coalesced permit starts a fresh fair discovery round after all accounts in
                // this round have released their scheduler slots.
                self.command_wake.wake();
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

    /// Notifications minimize commit-to-claim latency while the bounded poll remains the durable
    /// recovery fallback. The loop owns no in-memory command authority: stopping it merely leaves
    /// rows for the next singleton to recover.
    pub async fn run_until_shutdown(
        &mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), BinanceCommandLedgerError> {
        let mut activation = tokio::time::interval_at(
            tokio::time::Instant::now() + ACTIVATION_POLL_INTERVAL,
            ACTIVATION_POLL_INTERVAL,
        );
        activation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let _ = self.recover_commands_once().await?;
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = self.command_wake.notified() => {}
                () = tokio::time::sleep(EXECUTOR_POLL_INTERVAL) => {}
                _ = activation.tick() => {
                    // Activation performs only signed reads and CAS-protected PostgreSQL state
                    // changes. A newly committed order command may cancel this low-priority
                    // future; an already committed activation is durable and an unfinished
                    // transaction rolls back.
                    match select_activation_turn(
                        &mut shutdown,
                        self.command_wake.clone(),
                        self.process_pending_activations(),
                    )
                    .await
                    {
                        ActivationTurn::Stop => return Ok(()),
                        ActivationTurn::Retry | ActivationTurn::CommandWake => {}
                        ActivationTurn::Completed(result) => result?,
                    }
                }
            }
        }
    }
}

async fn select_activation_turn<F>(
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
    command_wake: CommandWake,
    activation: F,
) -> ActivationTurn<F::Output>
where
    F: Future,
{
    tokio::pin!(activation);
    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                ActivationTurn::Stop
            } else {
                ActivationTurn::Retry
            }
        }
        () = command_wake.notified() => ActivationTurn::CommandWake,
        result = &mut activation => ActivationTurn::Completed(result),
    }
}

/// Selects one account turn while retaining every unresolved sibling of the oldest Grid batch.
/// A batch claim commits all selected rows as `Sending` before the first transport call, so a
/// crash even before zero sends is intentionally fail-closed: absence is not proof of non-dispatch
/// and these rows must never return to `Pending`. Grouped recovery makes every sibling observable,
/// but resolving a genuinely absent command still requires a future claimed/prepared protocol or
/// explicit operator attention.
fn account_recovery_groups(
    commands: Vec<RecoverableBinanceCommand>,
) -> BTreeMap<String, Option<Vec<RecoverableBinanceCommand>>> {
    let mut accounts = BTreeMap::<String, Vec<RecoverableBinanceCommand>>::new();
    for command in commands {
        accounts
            .entry(command.trading_account_id.clone())
            .or_default()
            .push(command);
    }
    accounts
        .into_iter()
        .map(|(account, commands)| {
            let first = commands
                .iter()
                .find(|command| command.state != ExecutorCommandState::Pending);
            let Some(first) = first else {
                return (account, None);
            };
            let first_batch_id = first.grid_batch_id.clone();
            let mut selected = match first_batch_id.as_deref() {
                Some(batch_id) => commands
                    .into_iter()
                    .filter(|command| {
                        command.state != ExecutorCommandState::Pending
                            && command.grid_batch_id.as_deref() == Some(batch_id)
                    })
                    .collect::<Vec<_>>(),
                None => commands
                    .into_iter()
                    .find(|command| command.state != ExecutorCommandState::Pending)
                    .into_iter()
                    .collect(),
            };
            selected.sort_by_key(|command| command.dispatch_sequence.unwrap_or(0));
            (account, Some(selected))
        })
        .collect()
}

async fn execute_scheduled<E, S>(
    store: PgExecutorStore,
    mut exchange: E,
    secrets: Arc<S>,
    account: &str,
    scheduled: ScheduledCommand,
) -> Result<AccountDrainResult, BinanceCommandLedgerError>
where
    E: BinanceExecution + Send,
    S: ExecutorCredentials + Send + Sync,
{
    let mut next = Some(scheduled);
    let mut processed = 0_usize;
    let mut last_decision = AccountDrainDecision::Stop;
    while processed < ACCOUNT_DRAIN_QUANTUM {
        let current = match next.take() {
            Some(ScheduledCommand::Claim) => {
                let Some(command) = store.claim_next_command_batch(account, now_ms()?).await?
                else {
                    break;
                };
                ScheduledCommand::Claimed(command)
            }
            Some(command) => command,
            None => break,
        };
        last_decision = match current {
            ScheduledCommand::Claimed(command) => {
                submit_claimed(&store, &mut exchange, secrets.as_ref(), command).await?
            }
            ScheduledCommand::Reconcile(commands) => {
                reconcile_group(&store, &mut exchange, secrets.as_ref(), &commands).await?
            }
            ScheduledCommand::Claim => return Err(BinanceCommandLedgerError::Conflict),
        };
        processed = processed.saturating_add(1);
        if !continue_account_drain(processed, last_decision) {
            break;
        }
        next = Some(ScheduledCommand::Claim);
    }
    Ok(AccountDrainResult {
        processed,
        quantum_exhausted: processed == ACCOUNT_DRAIN_QUANTUM
            && last_decision == AccountDrainDecision::Continue,
    })
}

async fn submit_claimed<E, S>(
    store: &PgExecutorStore,
    exchange: &mut E,
    secrets: &S,
    mut batch: ClaimedBinanceBatch,
) -> Result<AccountDrainDecision, BinanceCommandLedgerError>
where
    E: BinanceExecution + Send,
    S: ExecutorCredentials + Sync,
{
    if batch.grid_batch_id.is_none() {
        if batch.grid_context.is_some() || batch.commands.len() != 1 {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let command = batch
            .commands
            .pop()
            .ok_or(BinanceCommandLedgerError::Conflict)?;
        return submit(store, exchange, secrets, command).await;
    }
    let grid_batch_id = batch
        .grid_batch_id
        .take()
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    if batch.commands.len() > MAX_ACCOUNT_QUEUE_DEPTH {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    submit_grid_batch(
        store,
        exchange,
        secrets,
        grid_batch_id,
        batch.grid_context.take(),
        batch.commands,
    )
    .await
}

async fn submit_grid_batch<E, S>(
    store: &PgExecutorStore,
    exchange: &mut E,
    secrets: &S,
    grid_batch_id: String,
    grid_context: Option<crate::kol_executor::GridBatchDispatchContext>,
    commands: Vec<ClaimedBinanceCommand>,
) -> Result<AccountDrainDecision, BinanceCommandLedgerError>
where
    E: BinanceExecution + Send,
    S: ExecutorCredentials + Sync,
{
    let first = commands
        .first()
        .ok_or(BinanceCommandLedgerError::Conflict)?;
    let credentials = match secrets
        .credentials(&first.credential_id, &first.owner_user_id)
        .await
    {
        Ok(credentials) => credentials,
        Err(_) => {
            reject_not_dispatched_batch(store, &commands, "credential_unavailable").await?;
            return Ok(AccountDrainDecision::Stop);
        }
    };
    let mut requests = Vec::with_capacity(commands.len());
    for command in &commands {
        if command.owner_user_id != first.owner_user_id
            || command.credential_id != first.credential_id
            || command.trading_account_id != first.trading_account_id
            || command.symbol != first.symbol
        {
            return Err(BinanceCommandLedgerError::Conflict);
        }
        let reservations = match store.reconciled_close_reservations(command).await {
            Ok(reservations) => reservations,
            Err(error) => {
                reject_not_dispatched_batch(store, &commands, ledger_not_dispatched_code(error))
                    .await?;
                return Ok(AccountDrainDecision::Stop);
            }
        };
        let mut child = request(command);
        child.reconciled_close_reservations = reservations;
        requests.push(child);
    }
    let execution_context = GridBatchExecutionContext {
        batch_id: grid_batch_id.clone(),
        owner_user_id: first.owner_user_id.clone(),
        durable: grid_context,
    };
    let exchange_started_ms = now_ms()?;
    let result = match exchange
        .submit_grid_batch(&execution_context, &requests, credentials)
        .await
    {
        Ok(result) => result,
        Err(error) => {
            let (state, code) = batch_failure_transition(error);
            match state {
                ExecutorCommandState::Rejected => {
                    reject_not_dispatched_batch(store, &commands, code).await?;
                }
                ExecutorCommandState::ReconcileRequired => {
                    mark_dispatch_uncertain_batch(store, &commands, code).await?;
                }
                _ => return Err(BinanceCommandLedgerError::Conflict),
            }
            return Ok(AccountDrainDecision::Stop);
        }
    };
    if result.commands.len() != commands.len()
        || result.timing.outbound_attempts as usize > commands.len()
        || !valid_batch_submit_timing(result.timing)
    {
        // The rows are already Sending, so an internal result mismatch must stop the singleton.
        // Restart recovery remains readback-only and cannot duplicate a POST.
        return Err(BinanceCommandLedgerError::Conflict);
    }
    tracing::info!(
        target: "venue_control::grid_dispatch",
        grid_batch_id = %grid_batch_id,
        command_count = commands.len(),
        outbound_attempts = result.timing.outbound_attempts,
        executor_start_to_first_submit_us = ?result.timing.executor_start_to_first_submit_us,
        executor_start_to_last_submit_us = ?result.timing.executor_start_to_last_submit_us,
        first_to_last_submit_us = ?result.timing.first_to_last_submit_us,
        "Binance Grid batch send-entry timing"
    );
    record_grid_event_to_send(&execution_context, exchange_started_ms, result.timing);
    let mut decision = AccountDrainDecision::Continue;
    for (command, child) in commands.iter().zip(result.commands) {
        let child_decision = match child {
            GridBatchCommandOutcome::Submitted(outcome) => {
                settle_submit_result(store, command, outcome).await?
            }
            GridBatchCommandOutcome::NotDispatched(error) => {
                store
                    .transition_command_with_readback(
                        &command.command_id,
                        ExecutorCommandState::Rejected,
                        now_ms()?,
                        Some(error.not_dispatched_code()),
                        command.native_order_id.as_deref(),
                    )
                    .await?;
                AccountDrainDecision::Continue
            }
        };
        if child_decision == AccountDrainDecision::Stop {
            decision = AccountDrainDecision::Stop;
        }
    }
    Ok(decision)
}

fn valid_batch_submit_timing(timing: crate::executor_exchange::GridBatchSubmitTiming) -> bool {
    match (
        timing.outbound_attempts,
        timing.executor_start_to_first_submit_us,
        timing.executor_start_to_last_submit_us,
        timing.first_to_last_submit_us,
    ) {
        (0, None, None, None) => true,
        (attempts, Some(first), Some(last), Some(span)) if attempts > 0 => {
            last >= first && span == last.saturating_sub(first)
        }
        _ => false,
    }
}

fn record_grid_event_to_send(
    context: &GridBatchExecutionContext,
    exchange_started_ms: u64,
    timing: crate::executor_exchange::GridBatchSubmitTiming,
) {
    let Some(source_event_received_ms) = context
        .durable
        .as_ref()
        .and_then(|durable| durable.source_event_received_ms)
    else {
        return;
    };
    let Some(base_us) = exchange_started_ms
        .checked_sub(source_event_received_ms)
        .and_then(|elapsed_ms| elapsed_ms.checked_mul(1_000))
    else {
        return;
    };
    let first_us = timing
        .executor_start_to_first_submit_us
        .and_then(|elapsed| base_us.checked_add(elapsed));
    let last_us = timing
        .executor_start_to_last_submit_us
        .and_then(|elapsed| base_us.checked_add(elapsed));
    tracing::info!(
        target: "venue_control::grid_hot_path",
        batch_id = %context.batch_id,
        event_to_first_send_entry_us = ?first_us,
        event_to_last_send_entry_us = ?last_us,
        first_within_target = first_us.is_some_and(|elapsed| elapsed <= 10_000),
        last_within_target = last_us.is_some_and(|elapsed| elapsed <= 10_000),
        "authenticated Grid fill to physical send-entry timing"
    );
}

async fn reject_not_dispatched_batch(
    store: &PgExecutorStore,
    commands: &[ClaimedBinanceCommand],
    code: &str,
) -> Result<(), BinanceCommandLedgerError> {
    for command in commands {
        store
            .transition_command_with_readback(
                &command.command_id,
                ExecutorCommandState::Rejected,
                now_ms()?,
                Some(code),
                command.native_order_id.as_deref(),
            )
            .await?;
    }
    Ok(())
}

async fn mark_dispatch_uncertain_batch(
    store: &PgExecutorStore,
    commands: &[ClaimedBinanceCommand],
    code: &str,
) -> Result<(), BinanceCommandLedgerError> {
    for command in commands {
        store
            .transition_command_with_readback(
                &command.command_id,
                ExecutorCommandState::ReconcileRequired,
                now_ms()?,
                Some(code),
                command.native_order_id.as_deref(),
            )
            .await?;
    }
    Ok(())
}

const fn batch_failure_transition(
    error: GridBatchSubmitError,
) -> (ExecutorCommandState, &'static str) {
    match error {
        GridBatchSubmitError::DefinitelyNotDispatched(error) => {
            (ExecutorCommandState::Rejected, error.not_dispatched_code())
        }
        GridBatchSubmitError::DispatchUncertain => (
            ExecutorCommandState::ReconcileRequired,
            "batch_dispatch_uncertain",
        ),
    }
}

async fn submit<E, S>(
    store: &PgExecutorStore,
    exchange: &mut E,
    secrets: &S,
    command: ClaimedBinanceCommand,
) -> Result<AccountDrainDecision, BinanceCommandLedgerError>
where
    E: BinanceExecution + Send,
    S: ExecutorCredentials + Sync,
{
    let credentials = match secrets
        .credentials(&command.credential_id, &command.owner_user_id)
        .await
    {
        Ok(credentials) => credentials,
        // Credential retrieval happens before the physical mutation boundary.
        Err(_) => {
            store
                .transition_command(
                    &command.command_id,
                    ExecutorCommandState::Rejected,
                    now_ms()?,
                    Some("credential_unavailable"),
                )
                .await?;
            return Ok(drain_after_persisted_state(ExecutorCommandState::Rejected));
        }
    };
    let reservations = match store.reconciled_close_reservations(&command).await {
        Ok(reservations) => reservations,
        Err(error) => {
            store
                .transition_command(
                    &command.command_id,
                    ExecutorCommandState::Rejected,
                    now_ms()?,
                    Some(ledger_not_dispatched_code(error)),
                )
                .await?;
            return Ok(drain_after_persisted_state(ExecutorCommandState::Rejected));
        }
    };
    let mut request = request(&command);
    request.reconciled_close_reservations = reservations;
    if crate::executor_exchange::is_terminal_open(&request)
        && !store.terminal_open_credential_verified(&command).await?
    {
        store
            .transition_command(
                &command.command_id,
                ExecutorCommandState::Rejected,
                now_ms()?,
                Some("credential_unavailable"),
            )
            .await?;
        return Ok(drain_after_persisted_state(ExecutorCommandState::Rejected));
    }
    match exchange.submit(&request, credentials).await {
        Ok(result) => settle_submit_result(store, &command, result).await,
        Err(error) => {
            let (state, code) = not_dispatched_transition(error);
            store
                .transition_command_with_readback(
                    &command.command_id,
                    state,
                    now_ms()?,
                    Some(code),
                    command.native_order_id.as_deref(),
                )
                .await?;
            Ok(drain_after_persisted_state(state))
        }
    }
}

async fn settle_submit_result(
    store: &PgExecutorStore,
    command: &ClaimedBinanceCommand,
    result: ExecutionOutcome,
) -> Result<AccountDrainDecision, BinanceCommandLedgerError> {
    if matches!(
        result.state,
        ExecutionReadback::Accepted | ExecutionReadback::Reconciled
    ) && result.native_order_id.is_none()
        && !matches!(&command.order, ClaimedBinanceOrder::CancelExact { .. })
    {
        store
            .transition_command_with_readback(
                &command.command_id,
                ExecutorCommandState::ReconcileRequired,
                now_ms()?,
                Some("signed_identity_missing"),
                command.native_order_id.as_deref(),
            )
            .await?;
        return Ok(drain_after_persisted_state(
            ExecutorCommandState::ReconcileRequired,
        ));
    }
    match result.state {
        ExecutionReadback::Rejected => {
            let reason = result
                .exchange_error_code
                .map(|code| format!("binance_{code}"));
            store
                .transition_command_with_readback(
                    &command.command_id,
                    ExecutorCommandState::Rejected,
                    now_ms()?,
                    Some(reason.as_deref().unwrap_or("binance_rejected")),
                    result.native_order_id.as_deref(),
                )
                .await?;
            Ok(drain_after_persisted_state(ExecutorCommandState::Rejected))
        }
        ExecutionReadback::Unknown => {
            store
                .transition_command_with_readback(
                    &command.command_id,
                    ExecutorCommandState::ReconcileRequired,
                    now_ms()?,
                    Some("dispatch_unknown"),
                    result.native_order_id.as_deref(),
                )
                .await?;
            Ok(drain_after_persisted_state(
                ExecutorCommandState::ReconcileRequired,
            ))
        }
        ExecutionReadback::Accepted | ExecutionReadback::Reconciled => {
            store
                .transition_command_with_readback(
                    &command.command_id,
                    ExecutorCommandState::Accepted,
                    now_ms()?,
                    None,
                    result.native_order_id.as_deref(),
                )
                .await?;
            if result.state == ExecutionReadback::Reconciled || completes_on_signed_accept(command)
            {
                store
                    .transition_command_with_readback(
                        &command.command_id,
                        ExecutorCommandState::Reconciled,
                        now_ms()?,
                        None,
                        result.native_order_id.as_deref(),
                    )
                    .await?;
                return Ok(drain_after_persisted_state(
                    ExecutorCommandState::Reconciled,
                ));
            }
            // A market order may still need a signed terminal/fill readback. The Accepted
            // transition installed its durable first deadline; discovery will read it when due.
            Ok(drain_after_persisted_state(ExecutorCommandState::Accepted))
        }
    }
}

async fn reconcile_group<E, S>(
    store: &PgExecutorStore,
    exchange: &mut E,
    secrets: &S,
    commands: &[RecoverableBinanceCommand],
) -> Result<AccountDrainDecision, BinanceCommandLedgerError>
where
    E: BinanceExecution + Send,
    S: ExecutorCredentials + Sync,
{
    if commands.is_empty() {
        return Err(BinanceCommandLedgerError::Conflict);
    }
    let recovery_ms = now_ms()?;
    let mut decision = AccountDrainDecision::Continue;
    for command in commands {
        if !command.reconciliation_due(recovery_ms)? {
            decision = AccountDrainDecision::Stop;
            continue;
        }
        if reconcile(store, exchange, secrets, command).await? == AccountDrainDecision::Stop {
            decision = AccountDrainDecision::Stop;
        }
    }
    Ok(decision)
}

async fn reconcile<E, S>(
    store: &PgExecutorStore,
    exchange: &mut E,
    secrets: &S,
    command: &RecoverableBinanceCommand,
) -> Result<AccountDrainDecision, BinanceCommandLedgerError>
where
    E: BinanceExecution + Send,
    S: ExecutorCredentials + Sync,
{
    if command.state == ExecutorCommandState::Sending {
        store
            .transition_command_with_readback(
                &command.command_id,
                ExecutorCommandState::ReconcileRequired,
                now_ms()?,
                Some("restart_reconcile"),
                command.native_order_id.as_deref(),
            )
            .await?;
        // The transition installs the first durable deadline. Do not turn restart discovery into
        // an immediate authenticated query burst.
        return Ok(drain_after_persisted_state(
            ExecutorCommandState::ReconcileRequired,
        ));
    }
    let credentials = match secrets
        .credentials(&command.credential_id, &command.owner_user_id)
        .await
    {
        Ok(credentials) => credentials,
        // Retain the durable uncertainty fence. A missing key is not evidence of absence, but it
        // must advance the durable schedule so every 100 ms discovery tick stays read-only.
        Err(_) => {
            store
                .defer_reconciliation(
                    command,
                    command.state,
                    now_ms()?,
                    Some("readback_credentials_unavailable"),
                    command.native_order_id.as_deref(),
                )
                .await?;
            return Ok(drain_after_persisted_state(command.state));
        }
    };
    readback_with_credentials(store, exchange, command, credentials).await
}

async fn readback_with_credentials<E>(
    store: &PgExecutorStore,
    exchange: &mut E,
    command: &RecoverableBinanceCommand,
    credentials: BinanceCredentials,
) -> Result<AccountDrainDecision, BinanceCommandLedgerError>
where
    E: BinanceExecution + Send,
{
    let result = exchange.readback(&request(command), credentials).await;
    match result {
        Ok(ExecutionOutcome {
            state: ExecutionReadback::Reconciled,
            native_order_id,
            ..
        }) => {
            store
                .transition_command_with_readback(
                    &command.command_id,
                    ExecutorCommandState::Reconciled,
                    now_ms()?,
                    None,
                    native_order_id.as_deref(),
                )
                .await?;
            Ok(drain_after_persisted_state(
                ExecutorCommandState::Reconciled,
            ))
        }
        Ok(ExecutionOutcome {
            state: ExecutionReadback::Accepted,
            native_order_id: Some(native_order_id),
            ..
        }) if completes_on_signed_accept(command) => {
            store
                .transition_command_with_readback(
                    &command.command_id,
                    ExecutorCommandState::Reconciled,
                    now_ms()?,
                    None,
                    Some(&native_order_id),
                )
                .await?;
            Ok(drain_after_persisted_state(
                ExecutorCommandState::Reconciled,
            ))
        }
        Ok(ExecutionOutcome {
            state: ExecutionReadback::Rejected,
            native_order_id,
            ..
        }) => {
            if command.state == ExecutorCommandState::Accepted {
                store
                    .transition_command_with_readback(
                        &command.command_id,
                        ExecutorCommandState::ReconcileRequired,
                        now_ms()?,
                        Some("signed_terminal_no_fill"),
                        native_order_id.as_deref(),
                    )
                    .await?;
            }
            store
                .transition_command_with_readback(
                    &command.command_id,
                    ExecutorCommandState::Rejected,
                    now_ms()?,
                    Some("binance_rejected"),
                    native_order_id.as_deref(),
                )
                .await?;
            Ok(drain_after_persisted_state(ExecutorCommandState::Rejected))
        }
        Ok(ExecutionOutcome {
            state: ExecutionReadback::Unknown,
            native_order_id,
            ..
        }) => {
            store
                .defer_reconciliation(
                    command,
                    ExecutorCommandState::ReconcileRequired,
                    now_ms()?,
                    Some("readback_unknown"),
                    native_order_id.as_deref(),
                )
                .await?;
            Ok(drain_after_persisted_state(
                ExecutorCommandState::ReconcileRequired,
            ))
        }
        Ok(ExecutionOutcome {
            state: ExecutionReadback::Accepted,
            native_order_id,
            ..
        }) => {
            store
                .defer_reconciliation(
                    command,
                    command.state,
                    now_ms()?,
                    None,
                    native_order_id.as_deref(),
                )
                .await?;
            Ok(drain_after_persisted_state(command.state))
        }
        Err(_) => {
            store
                .defer_reconciliation(
                    command,
                    ExecutorCommandState::ReconcileRequired,
                    now_ms()?,
                    Some("readback_unavailable"),
                    command.native_order_id.as_deref(),
                )
                .await?;
            Ok(drain_after_persisted_state(
                ExecutorCommandState::ReconcileRequired,
            ))
        }
    }
}

/// A post-only placement is durably complete once an independently signed exact readback proves
/// that the immutable order exists. Its later fill/cancel lifecycle belongs to the private
/// projection and order-ownership tables, so it must not monopolize the account command queue.
fn completes_on_signed_accept(command: &ClaimedBinanceCommand) -> bool {
    matches!(&command.order, ClaimedBinanceOrder::LimitPostOnly { .. })
}

fn request(command: &ClaimedBinanceCommand) -> ExecutionRequest {
    let order_kind = match &command.order {
        ClaimedBinanceOrder::Market {
            side,
            position_side,
            quantity,
            reducing,
        } => ExecutionOrderKind::Market {
            side: *side,
            position_side: *position_side,
            quantity: *quantity,
            reducing: *reducing,
        },
        ClaimedBinanceOrder::LimitPostOnly {
            side,
            position_side,
            quantity,
            price,
            reducing,
        } => ExecutionOrderKind::LimitPostOnly {
            side: *side,
            position_side: *position_side,
            quantity: *quantity,
            price: *price,
            reducing: *reducing,
        },
        ClaimedBinanceOrder::CancelExact {
            native_order_id,
            target_client_order_id,
        } => ExecutionOrderKind::CancelExact {
            native_order_id: native_order_id.clone(),
            target_client_order_id: target_client_order_id.clone(),
        },
    };
    ExecutionRequest {
        origin: command.origin,
        command_id: command.command_id.clone(),
        client_order_id: command.client_order_id.clone(),
        credential_id: command.credential_id.clone(),
        trading_account_id: command.trading_account_id.clone(),
        symbol: command.symbol.clone(),
        order_kind,
        known_native_order_id: command.native_order_id.clone(),
        reconciled_close_reservations: Vec::new(),
    }
}

const fn ledger_not_dispatched_code(error: BinanceCommandLedgerError) -> &'static str {
    match error {
        BinanceCommandLedgerError::Conflict => "not_dispatched_invalid",
        BinanceCommandLedgerError::Unavailable => "not_dispatched_unavailable",
    }
}

const fn not_dispatched_transition(
    error: crate::executor_exchange::BinanceExecutionError,
) -> (ExecutorCommandState, &'static str) {
    (ExecutorCommandState::Rejected, error.not_dispatched_code())
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

    #[derive(Clone)]
    struct StartedThenUncertain {
        started: Arc<std::sync::atomic::AtomicBool>,
    }

    impl BinanceExecution for StartedThenUncertain {
        fn submit<'a>(
            &'a mut self,
            _request: &'a ExecutionRequest,
            _credentials: BinanceCredentials,
        ) -> crate::executor_exchange::BinanceExecutionFuture<'a> {
            Box::pin(async { Err(crate::executor_exchange::BinanceExecutionError::Unavailable) })
        }

        fn readback<'a>(
            &'a mut self,
            _request: &'a ExecutionRequest,
            _credentials: BinanceCredentials,
        ) -> crate::executor_exchange::BinanceExecutionFuture<'a> {
            Box::pin(async { Err(crate::executor_exchange::BinanceExecutionError::Unavailable) })
        }

        fn submit_grid_batch<'a>(
            &'a mut self,
            _context: &'a GridBatchExecutionContext,
            _requests: &'a [ExecutionRequest],
            _credentials: BinanceCredentials,
        ) -> crate::executor_exchange::BinanceGridBatchFuture<'a> {
            let started = Arc::clone(&self.started);
            Box::pin(async move {
                started.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(GridBatchSubmitError::DispatchUncertain)
            })
        }
    }

    fn recoverable(
        account: &str,
        command_id: &str,
        batch_id: Option<&str>,
        dispatch_sequence: Option<u16>,
        state: ExecutorCommandState,
    ) -> Result<RecoverableBinanceCommand, Box<dyn std::error::Error>> {
        Ok(RecoverableBinanceCommand {
            command: ClaimedBinanceCommand {
                origin: venue_control_protocol::kol::ExecutorCommandOrigin::Terminal,
                command_id: command_id.into(),
                owner_user_id: "owner".into(),
                trading_account_id: account.into(),
                credential_id: "credential".into(),
                symbol: "BTC/USDT".parse()?,
                order: ClaimedBinanceOrder::LimitPostOnly {
                    side: venue_domain::domain::OrderSide::Buy,
                    position_side: venue_domain::domain::PositionSide::Long,
                    quantity: rust_decimal::Decimal::new(1, 3),
                    price: rust_decimal::Decimal::from(50_000),
                    reducing: false,
                },
                client_order_id: format!("client-{command_id}"),
                native_order_id: None,
                state,
            },
            grid_batch_id: batch_id.map(str::to_owned),
            dispatch_sequence,
            reconcile_attempts: 0,
            next_reconcile_ms: matches!(
                state,
                ExecutorCommandState::Accepted | ExecutorCommandState::ReconcileRequired
            )
            .then_some(1),
        })
    }

    #[tokio::test]
    async fn command_notification_interrupts_wait_and_coalesces_without_busy_loop()
    -> Result<(), Box<dyn std::error::Error>> {
        let wake = CommandWake::new();
        let waiter = wake.clone();
        let task = tokio::spawn(async move {
            waiter.notified().await;
        });
        tokio::task::yield_now().await;
        wake.wake();
        tokio::time::timeout(std::time::Duration::from_millis(100), task).await??;

        wake.wake();
        wake.wake();
        tokio::time::timeout(std::time::Duration::from_millis(100), wake.notified()).await?;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), wake.notified())
                .await
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn command_wake_cancels_a_low_priority_activation_turn() {
        struct DropProbe(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let (_shutdown_tx, mut shutdown) = tokio::sync::watch::channel(false);
        let wake = CommandWake::new();
        let signal = wake.clone();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = DropProbe(Arc::clone(&dropped));
        let task = tokio::spawn(async move {
            tokio::task::yield_now().await;
            signal.wake();
        });
        let turn = select_activation_turn(&mut shutdown, wake, async move {
            let _probe = probe;
            std::future::pending::<()>().await;
        })
        .await;
        task.await.expect("wake task");
        assert!(matches!(turn, ActivationTurn::CommandWake));
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn started_batch_top_level_failure_can_never_become_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut exchange = StartedThenUncertain {
            started: Arc::clone(&started),
        };
        let commands = [
            recoverable(
                "account-a",
                "place-1",
                Some("grid-batch"),
                Some(1),
                ExecutorCommandState::Sending,
            )?,
            recoverable(
                "account-a",
                "place-2",
                Some("grid-batch"),
                Some(2),
                ExecutorCommandState::Sending,
            )?,
        ];
        let requests = commands
            .iter()
            .map(|command| request(command))
            .collect::<Vec<_>>();
        let error = exchange
            .submit_grid_batch(
                &GridBatchExecutionContext {
                    batch_id: "grid-batch".to_owned(),
                    owner_user_id: "owner".to_owned(),
                    durable: None,
                },
                &requests,
                BinanceCredentials::from_secrets(
                    secrecy::SecretString::from("key".to_owned()),
                    secrecy::SecretString::from("secret".to_owned()),
                )?,
            )
            .await
            .expect_err("the fake must fail after entering dispatch");
        assert!(started.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            batch_failure_transition(error),
            (
                ExecutorCommandState::ReconcileRequired,
                "batch_dispatch_uncertain"
            )
        );
        assert_ne!(
            batch_failure_transition(error).0,
            ExecutorCommandState::Rejected
        );
        Ok(())
    }

    #[test]
    fn restart_groups_every_unresolved_grid_sibling_in_dispatch_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let groups = account_recovery_groups(vec![
            recoverable(
                "account-a",
                "batch-place-2",
                Some("grid-batch"),
                Some(2),
                ExecutorCommandState::Accepted,
            )?,
            recoverable(
                "account-a",
                "batch-place-1",
                Some("grid-batch"),
                Some(1),
                ExecutorCommandState::Sending,
            )?,
            recoverable(
                "account-a",
                "batch-cancel-3",
                Some("grid-batch"),
                Some(3),
                ExecutorCommandState::ReconcileRequired,
            )?,
            recoverable(
                "account-a",
                "later-batch",
                Some("later-grid-batch"),
                Some(1),
                ExecutorCommandState::Sending,
            )?,
            recoverable(
                "account-b",
                "pending",
                None,
                None,
                ExecutorCommandState::Pending,
            )?,
        ]);
        let selected = groups
            .get("account-a")
            .and_then(Option::as_deref)
            .ok_or("missing Grid recovery group")?;
        assert_eq!(
            selected
                .iter()
                .map(|command| command.command_id.as_str())
                .collect::<Vec<_>>(),
            ["batch-place-1", "batch-place-2", "batch-cancel-3"]
        );
        assert!(groups.get("account-b").is_some_and(Option::is_none));
        Ok(())
    }

    #[test]
    fn terminal_commands_continue_one_account_while_uncertainty_and_quantum_stop_it() {
        assert!(continue_account_drain(
            1,
            drain_after_persisted_state(ExecutorCommandState::Reconciled)
        ));
        assert!(continue_account_drain(
            2,
            drain_after_persisted_state(ExecutorCommandState::Rejected)
        ));
        assert!(!continue_account_drain(
            3,
            drain_after_persisted_state(ExecutorCommandState::ReconcileRequired)
        ));
        assert!(!continue_account_drain(
            3,
            drain_after_persisted_state(ExecutorCommandState::Accepted)
        ));
        assert!(!continue_account_drain(
            ACCOUNT_DRAIN_QUANTUM,
            drain_after_persisted_state(ExecutorCommandState::Reconciled)
        ));
    }

    #[test]
    fn adapter_errors_before_post_are_terminal_and_never_dispatch_unknown() {
        for (error, expected) in [
            (
                crate::executor_exchange::BinanceExecutionError::Invalid,
                "not_dispatched_invalid",
            ),
            (
                crate::executor_exchange::BinanceExecutionError::Unavailable,
                "not_dispatched_unavailable",
            ),
        ] {
            let (state, code) = not_dispatched_transition(error);
            assert_eq!(state, ExecutorCommandState::Rejected);
            assert_eq!(code, expected);
            assert_ne!(code, "dispatch_unknown");
        }
    }

    #[test]
    fn request_keeps_the_durable_identities_verbatim() -> Result<(), Box<dyn std::error::Error>> {
        let command = ClaimedBinanceCommand {
            origin: venue_control_protocol::kol::ExecutorCommandOrigin::Terminal,
            command_id: "command".into(),
            owner_user_id: "owner".into(),
            trading_account_id: "account".into(),
            credential_id: "credential".into(),
            symbol: "BTC/USDT".parse()?,
            order: ClaimedBinanceOrder::Market {
                side: venue_domain::domain::OrderSide::Buy,
                position_side: venue_domain::domain::PositionSide::Long,
                quantity: rust_decimal::Decimal::new(1, 3),
                reducing: false,
            },
            client_order_id: "client".into(),
            native_order_id: Some("native-1".into()),
            state: ExecutorCommandState::ReconcileRequired,
        };
        assert_eq!(
            request(&command),
            ExecutionRequest {
                origin: venue_control_protocol::kol::ExecutorCommandOrigin::Terminal,
                command_id: "command".into(),
                client_order_id: "client".into(),
                credential_id: "credential".into(),
                trading_account_id: "account".into(),
                symbol: "BTC/USDT".parse()?,
                order_kind: ExecutionOrderKind::Market {
                    side: venue_domain::domain::OrderSide::Buy,
                    position_side: venue_domain::domain::PositionSide::Long,
                    quantity: rust_decimal::Decimal::new(1, 3),
                    reducing: false,
                },
                known_native_order_id: Some("native-1".into()),
                reconciled_close_reservations: Vec::new(),
            }
        );
        assert!(!completes_on_signed_accept(&command));
        Ok(())
    }

    #[test]
    fn signed_post_only_acceptance_releases_the_account_queue()
    -> Result<(), Box<dyn std::error::Error>> {
        let command = ClaimedBinanceCommand {
            origin: venue_control_protocol::kol::ExecutorCommandOrigin::Terminal,
            command_id: "command".into(),
            owner_user_id: "owner".into(),
            trading_account_id: "account".into(),
            credential_id: "credential".into(),
            symbol: "BTC/USDT".parse()?,
            order: ClaimedBinanceOrder::LimitPostOnly {
                side: venue_domain::domain::OrderSide::Buy,
                position_side: venue_domain::domain::PositionSide::Long,
                quantity: rust_decimal::Decimal::new(1, 3),
                price: rust_decimal::Decimal::new(30_000, 0),
                reducing: false,
            },
            client_order_id: "client".into(),
            native_order_id: Some("native-1".into()),
            state: ExecutorCommandState::Accepted,
        };
        assert!(completes_on_signed_accept(&command));
        Ok(())
    }

    #[test]
    fn request_keeps_dual_exact_cancel_selectors_separate_from_command_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let command = ClaimedBinanceCommand {
            origin: venue_control_protocol::kol::ExecutorCommandOrigin::Terminal,
            command_id: "cancel-command".into(),
            owner_user_id: "owner".into(),
            trading_account_id: "account".into(),
            credential_id: "credential".into(),
            symbol: "BTC/USDT".parse()?,
            order: ClaimedBinanceOrder::CancelExact {
                native_order_id: Some("321".into()),
                target_client_order_id: Some("grid-owned-order".into()),
            },
            client_order_id: "cancel-request-id".into(),
            native_order_id: Some("321".into()),
            state: ExecutorCommandState::ReconcileRequired,
        };
        assert_eq!(
            request(&command),
            ExecutionRequest {
                origin: venue_control_protocol::kol::ExecutorCommandOrigin::Terminal,
                command_id: "cancel-command".into(),
                client_order_id: "cancel-request-id".into(),
                credential_id: "credential".into(),
                trading_account_id: "account".into(),
                symbol: "BTC/USDT".parse()?,
                order_kind: ExecutionOrderKind::CancelExact {
                    native_order_id: Some("321".into()),
                    target_client_order_id: Some("grid-owned-order".into()),
                },
                known_native_order_id: Some("321".into()),
                reconciled_close_reservations: Vec::new(),
            }
        );
        Ok(())
    }
}
