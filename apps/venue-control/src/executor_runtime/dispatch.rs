use super::*;
use std::collections::BTreeSet;

impl<E, S> BinanceExecutorRuntime<E, S>
where
    E: BinanceExecution + BinanceActivationBaseline + Clone + Send + 'static,
    S: ExecutorCredentials + Send + Sync + 'static,
{
    /// Discovery continues while slow account turns are in flight. The scheduler contains only
    /// account hints: the actual claim, revision gate and uncertain-order fence remain in SQL.
    pub async fn run_until_shutdown(
        &mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), BinanceCommandLedgerError> {
        let mut scheduler = AccountSerialScheduler::new(MAX_GLOBAL_IN_FLIGHT);
        let mut scheduled = BTreeSet::new();
        let mut failed_until = BTreeMap::new();
        let mut tasks = tokio::task::JoinSet::new();
        let mut task_accounts = BTreeMap::new();
        let mut activations = tokio::task::JoinSet::new();
        let mut discovery = tokio::time::interval(EXECUTOR_POLL_INTERVAL);
        discovery.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut activation = tokio::time::interval(ACTIVATION_POLL_INTERVAL);
        activation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut discover = true;
        loop {
            if *shutdown.borrow() {
                // Aborted sends retain Sending/Accepted and are read back by the next process.
                // No cancellation path rewrites them to Pending or authorizes a second POST.
                tasks.shutdown().await;
                activations.shutdown().await;
                return Ok(());
            }
            if discover {
                let now = now_ms()?;
                let groups = recover_account_groups(&self.store).await?;
                failed_until.retain(|_, until| *until > now);
                for (account, unresolved) in groups {
                    if scheduled.contains(&account) || failed_until.contains_key(&account) {
                        continue;
                    }
                    // Admission remains bounded. Excess accounts stay durable and are discovered
                    // as soon as a slot is released, rather than being dropped or failing all KOLs.
                    if scheduled.len() >= crate::kol_executor::MAX_ENABLED_FOLLOWERS {
                        break;
                    }
                    let command = match unresolved {
                        Some(commands) => {
                            let due = commands.iter().try_fold(false, |due, command| {
                                command.reconciliation_due(now).map(|next| due || next)
                            });
                            match due {
                                Ok(true) => ScheduledCommand::Reconcile(commands),
                                Ok(false) => continue,
                                Err(_) => {
                                    failed_until.insert(account, now.saturating_add(8_000));
                                    tracing::warn!(
                                        "Binance account has an invalid reconciliation schedule"
                                    );
                                    continue;
                                }
                            }
                        }
                        None => ScheduledCommand::Claim,
                    };
                    scheduler.enqueue(account.clone(), command)?;
                    scheduled.insert(account);
                }
                discover = false;
            }
            while let Some(queued) = scheduler.claim_next() {
                let store = self.store.clone();
                let exchange = self.exchange.clone();
                let secrets = Arc::clone(&self.secrets);
                let account = queued.trading_account_id;
                let worker_account = account.clone();
                let task = tasks.spawn(async move {
                    execute_scheduled(store, exchange, secrets, &worker_account, queued.command)
                        .await
                });
                task_accounts.insert(task.id(), account);
            }
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tasks.shutdown().await;
                        activations.shutdown().await;
                        return Ok(());
                    }
                }
                joined = tasks.join_next_with_id(), if !tasks.is_empty() => {
                    let (id, result) = match joined {
                        Some(Ok(value)) => value,
                        Some(Err(error)) => (error.id(), Err(BinanceCommandLedgerError::Unavailable)),
                        None => return Err(BinanceCommandLedgerError::Unavailable),
                    };
                    let account = task_accounts.remove(&id).ok_or(BinanceCommandLedgerError::Conflict)?;
                    scheduler.settle(&account)?;
                    scheduled.remove(&account);
                    if result.is_err() {
                        failed_until.insert(account, now_ms()?.saturating_add(8_000));
                        tracing::warn!("Binance account turn failed; durable work retained for reconciliation");
                    } else if result.is_ok_and(|turn| turn.processed > 0) {
                        self.command_wake.wake();
                    }
                }
                completed = activations.join_next(), if !activations.is_empty() => {
                    if !matches!(completed, Some(Ok(Ok(())))) {
                        tracing::warn!("Binance activation turn failed; order dispatch remains independent");
                    }
                }
                _ = activation.tick(), if activations.is_empty() => {
                    let mut worker = Self {
                        store: self.store.clone(), exchange: self.exchange.clone(),
                        secrets: Arc::clone(&self.secrets), command_wake: self.command_wake.clone(),
                    };
                    activations.spawn(async move { worker.process_pending_activations().await });
                }
                () = self.command_wake.notified() => discover = true,
                _ = discovery.tick() => discover = true,
            }
        }
    }
}
