use std::{collections::VecDeque, future::Future};

use tokio::sync::{mpsc, watch};
use venue_control_protocol::grid::GridInstanceState;

use super::{
    BinanceGridRuntime, BinanceGridRuntimeError, GRID_TICK_INTERVAL, GridPrivateStreamSignal,
    fast_path::receive_private_signal, now_ms,
};

enum ColdTurn<T> {
    Stop,
    Retry,
    Private(Option<GridPrivateStreamSignal>),
    Completed(T),
}

impl BinanceGridRuntime {
    pub async fn run_until_shutdown(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), BinanceGridRuntimeError> {
        let mut interval = tokio::time::interval(GRID_TICK_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut private_stream = self.hot_path.take_receiver();
        let mut deferred = VecDeque::new();
        let mut cold_due = false;
        let mut next_rejection_check = tokio::time::Instant::now();

        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            // A continuously ready private mailbox can cancel every cold turn. The durable
            // rejection deadline must still be checked before draining that mailbox.
            if tokio::time::Instant::now() >= next_rejection_check {
                if let Err(error) = self.enforce_rejection_deadlines().await {
                    tracing::warn!(target: "venue_control::grid_runtime", %error,
                        "Grid rejection deadline check failed and will retry");
                }
                next_rejection_check = tokio::time::Instant::now() + GRID_TICK_INTERVAL;
            }
            if let Some(signal) = deferred.pop_front() {
                self.handle_private_signal(signal, &mut private_stream, &mut deferred)
                    .await;
                continue;
            }
            if cold_due {
                // Cancellation only drops this Grid coordinator future. It performs signed/public
                // reads and idempotent or CAS-protected PostgreSQL commits, never a physical order
                // send. A completed commit remains the next turn's durable input; an unfinished
                // SQL transaction rolls back. The account-serial Executor exclusively owns sends.
                let turn =
                    select_cold_turn(&mut shutdown, &mut private_stream, self.run_once()).await;
                match turn {
                    ColdTurn::Stop => return Ok(()),
                    ColdTurn::Retry => {}
                    ColdTurn::Private(Some(signal)) => {
                        self.handle_private_signal(signal, &mut private_stream, &mut deferred)
                            .await;
                    }
                    ColdTurn::Private(None) => private_stream = None,
                    ColdTurn::Completed(Ok(_)) => cold_due = false,
                    ColdTurn::Completed(Err(error)) => {
                        eprintln!("Binance Grid cold turn failed and will retry: {error}");
                        tracing::warn!(
                            target: "venue_control::grid_runtime",
                            error = %error,
                            "Binance Grid cold turn failed and will retry"
                        );
                        cold_due = false;
                    }
                }
                continue;
            }

            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                signal = receive_private_signal(&mut private_stream) => {
                    match signal {
                        Some(signal) => self
                            .handle_private_signal(signal, &mut private_stream, &mut deferred)
                            .await,
                        None => private_stream = None,
                    }
                }
                _ = interval.tick() => cold_due = true,
            }
        }
    }

    pub(super) async fn enforce_rejection_deadlines(&self) -> Result<(), BinanceGridRuntimeError> {
        for record in self.store.list_runtime_instances().await? {
            if !matches!(
                record.instance.state,
                GridInstanceState::StartPending
                    | GridInstanceState::Running
                    | GridInstanceState::Blocked
            ) {
                continue;
            }
            let first = self
                .store
                .exchange_rejection_started_ms(
                    &record.instance.instance_id,
                    record.instance.config_revision,
                )
                .await?;
            let now = now_ms()?;
            if crate::grid_store::rejection::rejection_reset_due(first, now) {
                match self
                    .store
                    .settle_runtime_state_checked(
                        &record.instance.instance_id,
                        Some(record.instance.revision),
                        record.instance.state,
                        GridInstanceState::ResetRequired,
                        Some("exchange_rejection_delay_elapsed"),
                        now,
                    )
                    .await
                {
                    Ok(_) | Err(crate::GridStoreError::Conflict) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(())
    }
}

async fn select_cold_turn<F>(
    shutdown: &mut watch::Receiver<bool>,
    private_stream: &mut Option<mpsc::Receiver<GridPrivateStreamSignal>>,
    cold: F,
) -> ColdTurn<F::Output>
where
    F: Future,
{
    tokio::pin!(cold);
    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                ColdTurn::Stop
            } else {
                ColdTurn::Retry
            }
        }
        signal = receive_private_signal(private_stream) => ColdTurn::Private(signal),
        result = &mut cold => ColdTurn::Completed(result),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn invalidate() -> GridPrivateStreamSignal {
        GridPrivateStreamSignal::Invalidate {
            credential_id: "credential".to_owned(),
        }
    }

    #[tokio::test]
    async fn ready_private_signal_cancels_a_pending_cold_turn()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let (private_tx, private_rx) = mpsc::channel(1);
        private_tx.send(invalidate()).await?;
        let drops = Arc::new(AtomicUsize::new(0));
        let probe = DropProbe(Arc::clone(&drops));
        let cold = async move {
            let _probe = probe;
            std::future::pending::<usize>().await
        };
        let mut private_stream = Some(private_rx);

        let turn = select_cold_turn(&mut shutdown, &mut private_stream, cold).await;
        assert!(matches!(turn, ColdTurn::Private(Some(_))));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn private_signal_wins_when_cold_completion_is_already_ready_then_cold_can_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_shutdown_tx, mut shutdown) = watch::channel(false);
        let (private_tx, private_rx) = mpsc::channel(1);
        private_tx.send(invalidate()).await?;
        let mut private_stream = Some(private_rx);

        let first = select_cold_turn(
            &mut shutdown,
            &mut private_stream,
            std::future::ready(1_usize),
        )
        .await;
        assert!(matches!(first, ColdTurn::Private(Some(_))));
        let retried = select_cold_turn(
            &mut shutdown,
            &mut private_stream,
            std::future::ready(2_usize),
        )
        .await;
        assert!(matches!(retried, ColdTurn::Completed(2)));
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_wins_over_private_signal_and_ready_cold_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let (shutdown_tx, mut shutdown) = watch::channel(false);
        let (private_tx, private_rx) = mpsc::channel(1);
        private_tx.send(invalidate()).await?;
        shutdown_tx.send(true)?;
        let mut private_stream = Some(private_rx);

        let turn = select_cold_turn(
            &mut shutdown,
            &mut private_stream,
            std::future::ready(1_usize),
        )
        .await;
        assert!(matches!(turn, ColdTurn::Stop));
        Ok(())
    }
}
