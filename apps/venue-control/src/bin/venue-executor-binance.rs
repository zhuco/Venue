//! Singleton process entrypoint for the Binance KOL executor.
//!
//! This binary assembles the singleton's restricted PostgreSQL and master-key boundaries. It
//! never reads Binance API secrets from environment variables.

use sqlx::postgres::PgPoolOptions;
use std::{collections::BTreeMap, future::Future};
use venue_control::{
    BinanceExecutorSingleton, BinanceGridRuntime, BinanceGridStore,
    GRID_PRIVATE_STREAM_CHANNEL_CAPACITY, GridHotDispatchCache, GridPrivateStreamSignal,
    accounts::CredentialCipher,
    executor_config::ExecutorLaunchConfig,
    executor_exchange::BinanceExecutionRouter,
    executor_runtime::{BinanceExecutorRuntime, CommandWake, ExecutorCredentials},
    executor_secret::ExecutorSecretProvider,
    executor_store::PgExecutorStore,
    kol_executor::{source_fill_from_private, source_fill_from_signed},
    private_projection::{
        ActiveProjectionSource, BinancePrivateProjectionStore, PRIVATE_STREAM_FILL_BATCH_LIMIT,
    },
};
use venue_execution::SignedAccountSnapshot;
use venue_gateway_binance::{
    BinancePrivateAccountEvent, BinancePrivateFillEvent, BinanceTransportLimits, GatewayBinding,
    GatewayMode, VenueId,
};

const EXECUTOR_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const EXECUTOR_MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const PROJECTION_DISCOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);
const STREAM_PROJECTION_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
const GRID_PRIVATE_RECOVERY_CHANNEL_CAPACITY: usize = 32;
const MAX_PROJECTION_RETRY_SHIFT: u32 = 5;
const PRIVATE_STREAM_FILL_BATCH_WINDOW: std::time::Duration = std::time::Duration::from_millis(1);
const GRID_FILL_SETTLEMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const PROJECTION_PERSISTENCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    tracing_subscriber::registry()
        .with(tracing_subscriber::filter::filter_fn(|metadata| {
            *metadata.level() <= tracing::Level::WARN
                || matches!(
                    metadata.target(),
                    "venue_control::grid_hot_path" | "venue_control::grid_dispatch"
                )
        }))
        .with(tracing_subscriber::fmt::layer().with_ansi(false))
        .init();
    let launch = ExecutorLaunchConfig::from_environment()?;
    let singleton = BinanceExecutorSingleton::acquire(&launch.database_url).await?;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&launch.database_url)
        .await?;
    let store = PgExecutorStore::new(pool.clone());
    let projection_store = BinancePrivateProjectionStore::new(pool.clone());
    let grid_store = BinanceGridStore::new(pool.clone());
    let secrets = ExecutorSecretProvider::new(pool, CredentialCipher::from_environment()?);
    let hot_dispatch = GridHotDispatchCache::new();
    let exchange = BinanceExecutionRouter::with_hot_dispatch(
        BinanceTransportLimits::new(EXECUTOR_HTTP_TIMEOUT, EXECUTOR_MAX_RESPONSE_BYTES)?,
        hot_dispatch.clone(),
    );
    let command_wake = CommandWake::new();
    let mut runtime = BinanceExecutorRuntime::with_command_wake(
        store.clone(),
        exchange.clone(),
        secrets.clone(),
        command_wake.clone(),
    );
    // Promotion is a signed, fail-closed activation baseline; recovery then fences all
    // nonterminal commands before this process establishes any KOL event source.
    runtime.recover_once().await?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let clock_router = exchange.clone();
    let clock_shutdown = shutdown_rx.clone();
    let clock_task =
        tokio::spawn(async move { clock_router.maintain_clocks(clock_shutdown).await });
    let (grid_signal_tx, grid_signal_rx) =
        tokio::sync::mpsc::channel(GRID_PRIVATE_STREAM_CHANNEL_CAPACITY);
    let (grid_recovery_tx, grid_recovery_rx) =
        tokio::sync::mpsc::channel(GRID_PRIVATE_RECOVERY_CHANNEL_CAPACITY);
    tokio::spawn(run_projection_supervisor(
        store.clone(),
        projection_store.clone(),
        secrets.clone(),
        command_wake.clone(),
        hot_dispatch.clone(),
        exchange,
        grid_signal_tx,
        grid_recovery_rx,
        shutdown_rx.clone(),
    ));
    let grid_runtime = BinanceGridRuntime::with_private_stream(
        grid_store,
        projection_store,
        BinanceTransportLimits::new(EXECUTOR_HTTP_TIMEOUT, EXECUTOR_MAX_RESPONSE_BYTES)?,
        grid_signal_rx,
        grid_recovery_tx,
        command_wake,
        hot_dispatch,
    )
    .with_risk_credentials(secrets.clone());
    let grid_task = tokio::spawn(grid_runtime.run_until_shutdown(shutdown_rx.clone()));
    let signal_shutdown = shutdown_tx.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = signal_shutdown.send(true);
        }
    });
    #[cfg(unix)]
    {
        let signal_shutdown = shutdown_tx.clone();
        tokio::spawn(async move {
            if let Ok(mut terminate) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                terminate.recv().await;
                let _ = signal_shutdown.send(true);
            }
        });
    }
    let result = runtime.run_until_shutdown(shutdown_rx).await;
    let _ = shutdown_tx.send(true);
    clock_task.await?;
    grid_task.await??;
    singleton.release().await?;
    result?;
    Ok(())
}

enum ProjectionMessage {
    StreamSnapshot {
        worker_id: u64,
        source: ActiveProjectionSource,
        snapshot: SignedAccountSnapshot,
        completion: std::sync::mpsc::SyncSender<Result<Option<bool>, ()>>,
    },
    Invalidate {
        credential_id: String,
        worker_id: u64,
    },
    StreamFills {
        worker_id: u64,
        source: ActiveProjectionSource,
        events: Vec<BinancePrivateFillEvent>,
        completion: std::sync::mpsc::SyncSender<bool>,
    },
    Snapshot {
        worker_id: u64,
        source: ActiveProjectionSource,
        snapshot: SignedAccountSnapshot,
        completion: std::sync::mpsc::SyncSender<bool>,
    },
    PersistenceSettled {
        credential_id: String,
        worker_id: u64,
        healthy: bool,
        completion: std::sync::mpsc::SyncSender<bool>,
    },
    Stopped {
        credential_id: String,
        worker_id: u64,
    },
}

struct ProjectionWorker {
    id: u64,
    source: ActiveProjectionSource,
    stop: tokio::sync::watch::Sender<bool>,
    reconcile: std::sync::mpsc::SyncSender<()>,
    persistence_in_flight: bool,
}

async fn run_projection_supervisor(
    executor_store: PgExecutorStore,
    projection_store: BinancePrivateProjectionStore,
    secrets: ExecutorSecretProvider,
    command_wake: CommandWake,
    hot_dispatch: GridHotDispatchCache,
    exchange: BinanceExecutionRouter,
    grid_signal: tokio::sync::mpsc::Sender<GridPrivateStreamSignal>,
    mut grid_recovery: tokio::sync::mpsc::Receiver<String>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(32);
    let mut workers = BTreeMap::<String, ProjectionWorker>::new();
    let mut next_worker_id = 0_u64;
    let mut discovery = tokio::time::interval(PROJECTION_DISCOVERY_INTERVAL);
    let mut recovery_open = true;
    let mut persistence_tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    for (_, worker) in workers { let _ = worker.stop.send(true); }
                    persistence_tasks.abort_all();
                    return;
                }
            }
            message = message_rx.recv() => match message {
                Some(ProjectionMessage::Invalidate { credential_id, worker_id }) => {
                    if is_current_worker(&workers, &credential_id, worker_id) {
                        hot_dispatch.invalidate_credential(&credential_id);
                        let _ = projection_store.invalidate_stream(&credential_id).await;
                        let _ = grid_signal
                            .try_send(GridPrivateStreamSignal::Invalidate { credential_id });
                    }
                }
                Some(ProjectionMessage::StreamFills {
                    worker_id,
                    source,
                    events,
                    completion,
                }) => {
                    if begin_projection_persistence(
                        &mut workers,
                        &source.credential_id,
                        worker_id,
                    ) {
                        let executor_store = executor_store.clone();
                        let projection_store = projection_store.clone();
                        let grid_signal = grid_signal.clone();
                        let command_wake = command_wake.clone();
                        let hot_dispatch = hot_dispatch.clone();
                        let message_tx = message_tx.clone();
                        persistence_tasks.spawn(async move {
                            let outcome = tokio::time::timeout(
                                PROJECTION_PERSISTENCE_TIMEOUT,
                                async {
                                    match now_ms() {
                                        Ok(now) => persist_stream_fill_batch_turn(
                                            &executor_store,
                                            &projection_store,
                                            &grid_signal,
                                            &source,
                                            &events,
                                            now,
                                        )
                                        .await,
                                        Err(_) => StreamFillBatchOutcome::FailedBeforePersistence,
                                    }
                                },
                            )
                            .await
                            .unwrap_or(StreamFillBatchOutcome::FailedBeforePersistence);
                            if outcome.should_wake_executor() {
                                command_wake.wake();
                            }
                            if !outcome.worker_healthy() {
                                hot_dispatch.invalidate_credential(&source.credential_id);
                                let _ = grid_signal.try_send(GridPrivateStreamSignal::Invalidate {
                                    credential_id: source.credential_id.clone(),
                                });
                            }
                            let _ = message_tx
                                .send(ProjectionMessage::PersistenceSettled {
                                    credential_id: source.credential_id,
                                    worker_id,
                                    healthy: outcome.worker_healthy(),
                                    completion,
                                })
                                .await;
                        });
                    } else {
                        let _ = completion.send(false);
                    }
                }
                Some(ProjectionMessage::StreamSnapshot { worker_id, source, snapshot, completion }) => {
                    if !is_current_worker(&workers, &source.credential_id, worker_id) {
                        let _ = completion.send(Err(()));
                        continue;
                    }
                    let ready = projection_store.stream_surface_settled(&source, &snapshot).await;
                    if !matches!(ready, Ok(Some(true))) {
                        let _ = completion.send(ready.map_err(|_| ()));
                        continue;
                    }
                    let persisted = match now_ms() {
                        Ok(now) => projection_store.persist(&source, &snapshot, now).await.is_ok(),
                        Err(_) => false,
                    };
                    if !persisted { let _ = completion.send(Err(())); continue; }
                    let (ready, settled) = tokio::sync::oneshot::channel();
                    let warmed = tokio::time::timeout(PROJECTION_PERSISTENCE_TIMEOUT, async {
                        grid_signal.send(GridPrivateStreamSignal::ProjectionReady { credential_id: source.credential_id, completion: ready }).await.map_err(|_| ())?;
                        settled.await.map_err(|_| ())
                    }).await;
                    let _ = completion.send(if matches!(warmed, Ok(Ok(true))) { Ok(Some(true)) } else { Err(()) });
                }
                Some(ProjectionMessage::Snapshot {
                    worker_id,
                    source,
                    snapshot,
                    completion,
                }) => {
                    if begin_projection_persistence(
                        &mut workers,
                        &source.credential_id,
                        worker_id,
                    ) {
                        let executor_store = executor_store.clone();
                        let projection_store = projection_store.clone();
                        let grid_signal = grid_signal.clone();
                        let command_wake = command_wake.clone();
                        let hot_dispatch = hot_dispatch.clone();
                        let message_tx = message_tx.clone();
                        let exchange = exchange.clone();
                        persistence_tasks.spawn(async move {
                            hot_dispatch.invalidate_credential(&source.credential_id);
                            let _ = grid_signal.try_send(GridPrivateStreamSignal::Invalidate {
                                credential_id: source.credential_id.clone(),
                            });
                            let persisted = tokio::time::timeout(
                                PROJECTION_PERSISTENCE_TIMEOUT,
                                async {
                                    if exchange.prepare_account_transports(&source.trading_account_id, &source.symbols).await.is_err() {
                                        return false;
                                    }
                                    match now_ms() {
                                        Ok(now) => persist_projection_turn(
                                            &executor_store,
                                            &projection_store,
                                            &source,
                                            &snapshot,
                                            now,
                                        )
                                        .await,
                                        Err(_) => false,
                                    }
                                },
                            )
                            .await
                            .unwrap_or(false);
                            if persisted {
                                command_wake.wake();
                            }
                            let _ = message_tx
                                .send(ProjectionMessage::PersistenceSettled {
                                    credential_id: source.credential_id,
                                    worker_id,
                                    healthy: persisted,
                                    completion,
                                })
                                .await;
                        });
                    } else {
                        let _ = completion.send(false);
                    }
                }
                Some(ProjectionMessage::PersistenceSettled {
                    credential_id,
                    worker_id,
                    healthy,
                    completion,
                }) => {
                    let current = finish_projection_persistence(
                        &mut workers,
                        &credential_id,
                        worker_id,
                        healthy,
                    );
                    let _ = completion.send(current && healthy);
                }
                Some(ProjectionMessage::Stopped { credential_id, worker_id }) => {
                    if is_current_worker(&workers, &credential_id, worker_id) {
                        hot_dispatch.invalidate_credential(&credential_id);
                        let _ = projection_store.invalidate_stream(&credential_id).await;
                        let _ = grid_signal
                            .try_send(GridPrivateStreamSignal::Invalidate {
                                credential_id: credential_id.clone(),
                            });
                        workers.remove(&credential_id);
                    }
                }
                None => {
                    persistence_tasks.abort_all();
                    return;
                },
            },
            Some(_) = persistence_tasks.join_next(), if !persistence_tasks.is_empty() => {}
            recovery = grid_recovery.recv(), if recovery_open => match recovery {
                Some(credential_id) => {
                    if let Some(worker) = workers.get(&credential_id) {
                        let _ = worker.reconcile.try_send(());
                    }
                }
                None => recovery_open = false,
            },
            _ = discovery.tick() => {
                let Ok(now) = now_ms() else { continue; };
                let Ok(active) = projection_store.active_sources(now).await else { continue; };
                let active_by_id = active.into_iter().map(|source| (source.credential_id.clone(), source)).collect::<BTreeMap<_, _>>();
                let stale = workers.keys().filter(|id| !active_by_id.contains_key(*id)).cloned().collect::<Vec<_>>();
                for id in stale {
                    let in_flight = workers.get(&id).is_some_and(|worker| worker.persistence_in_flight);
                    hot_dispatch.invalidate_credential(&id);
                    if let Some(worker) = workers.get(&id) { let _ = worker.stop.send(true); }
                    if !in_flight { workers.remove(&id); }
                }
                for (credential_id, source) in active_by_id {
                    if workers.get(&credential_id).is_some_and(|worker| same_subscription(&worker.source, &source)) { continue; }
                    if workers.get(&credential_id).is_some_and(|worker| worker.persistence_in_flight) {
                        hot_dispatch.invalidate_credential(&credential_id);
                        if let Some(worker) = workers.get(&credential_id) { let _ = worker.stop.send(true); }
                        continue;
                    }
                    if let Some(worker) = workers.remove(&credential_id) { let _ = worker.stop.send(true); }
                    let Ok(credentials) = secrets.credentials(&source.credential_id, &source.owner_user_id).await else { continue; };
                    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
                    let (reconcile_tx, reconcile_rx) = std::sync::mpsc::sync_channel(1);
                    next_worker_id = next_worker_id.saturating_add(1);
                    let worker_id = next_worker_id;
                    workers.insert(credential_id, ProjectionWorker { id: worker_id, source: source.clone(), stop: stop_tx, reconcile: reconcile_tx, persistence_in_flight: false });
                    spawn_projection_worker(worker_id, source, credentials, stop_rx, reconcile_rx, hot_dispatch.clone(), message_tx.clone());
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StreamFillBatchOutcome {
    FailedBeforePersistence,
    Persisted {
        planned_commands: bool,
        worker_healthy: bool,
    },
}

impl StreamFillBatchOutcome {
    const fn should_wake_executor(self) -> bool {
        matches!(
            self,
            Self::Persisted {
                planned_commands: true,
                ..
            }
        )
    }

    const fn worker_healthy(self) -> bool {
        matches!(
            self,
            Self::Persisted {
                worker_healthy: true,
                ..
            }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PostPersistDispatch {
    planned_commands: bool,
    worker_healthy: bool,
}

/// The account-fill burst is the first durable boundary. Grid settles the complete persisted burst
/// before KOL planning starts, so follower work cannot win the shared account queue or delay Grid
/// repair. Any post-persist failure retires the worker for signed correction.
async fn persist_stream_fill_batch_turn(
    executor_store: &PgExecutorStore,
    projection_store: &BinancePrivateProjectionStore,
    grid_signal: &tokio::sync::mpsc::Sender<GridPrivateStreamSignal>,
    source: &ActiveProjectionSource,
    events: &[BinancePrivateFillEvent],
    now_ms: u64,
) -> StreamFillBatchOutcome {
    tracing::info!(target: "venue_control::grid_hot_path", fill_count = events.len(), "Authenticated fill burst received for persistence");
    if let Err(error) = projection_store.persist_stream_fills(source, events).await {
        tracing::warn!(target: "venue_control::grid_hot_path", error = %error, "Authenticated fill persistence failed before Grid planning");
        return StreamFillBatchOutcome::FailedBeforePersistence;
    }
    let store = executor_store.clone();
    let kol_user_id = source.kol_user_id.clone();
    let trading_account_id = source.trading_account_id.clone();
    let dispatched = dispatch_persisted_stream_batch(grid_signal, source, events, move |event| {
        let store = store.clone();
        let kol_user_id = kol_user_id.clone();
        let trading_account_id = trading_account_id.clone();
        async move {
            let Some(kol_user_id) = kol_user_id else {
                return Ok(false);
            };
            let normalized =
                source_fill_from_private(&trading_account_id, &event).map_err(|_| ())?;
            store
                .record_source_fill_and_plan(&kol_user_id, &normalized, now_ms)
                .await
                .map(|commands| !commands.is_empty())
                .map_err(|_| ())
        }
    })
    .await;
    StreamFillBatchOutcome::Persisted {
        planned_commands: dispatched.planned_commands,
        worker_healthy: dispatched.worker_healthy,
    }
}

async fn dispatch_persisted_stream_batch<F, Fut>(
    grid_signal: &tokio::sync::mpsc::Sender<GridPrivateStreamSignal>,
    source: &ActiveProjectionSource,
    events: &[BinancePrivateFillEvent],
    plan_kol: F,
) -> PostPersistDispatch
where
    F: FnMut(BinancePrivateFillEvent) -> Fut,
    Fut: Future<Output = Result<bool, ()>>,
{
    dispatch_persisted_stream_batch_with_timeout(
        grid_signal,
        source,
        events,
        GRID_FILL_SETTLEMENT_TIMEOUT,
        plan_kol,
    )
    .await
}

async fn dispatch_persisted_stream_batch_with_timeout<F, Fut>(
    grid_signal: &tokio::sync::mpsc::Sender<GridPrivateStreamSignal>,
    source: &ActiveProjectionSource,
    events: &[BinancePrivateFillEvent],
    settlement_timeout: std::time::Duration,
    mut plan_kol: F,
) -> PostPersistDispatch
where
    F: FnMut(BinancePrivateFillEvent) -> Fut,
    Fut: Future<Output = Result<bool, ()>>,
{
    let (completion, settled) = tokio::sync::oneshot::channel();
    if !matches!(
        tokio::time::timeout(settlement_timeout, async {
            grid_signal
                .send(GridPrivateStreamSignal::FillBatch {
                    source: source.clone(),
                    events: events.to_vec(),
                    completion,
                })
                .await
                .map_err(|_| ())?;
            settled.await.map_err(|_| ())
        })
        .await,
        Ok(Ok(true))
    ) {
        return PostPersistDispatch {
            planned_commands: false,
            worker_healthy: false,
        };
    }
    let mut planned_commands = false;
    for event in events.iter().cloned() {
        match plan_kol(event).await {
            Ok(planned) => planned_commands |= planned,
            Err(()) => {
                return PostPersistDispatch {
                    planned_commands,
                    worker_healthy: false,
                };
            }
        }
    }
    PostPersistDispatch {
        planned_commands,
        worker_healthy: true,
    }
}

/// KOL source fills are committed before the account projection advances its signed fill cursor.
/// A failure at either boundary retires the worker; discovery then rebuilds it from the unchanged
/// PostgreSQL projection cursor, and native trade IDs make any already committed overlap harmless.
async fn persist_projection_turn(
    executor_store: &PgExecutorStore,
    projection_store: &BinancePrivateProjectionStore,
    source: &ActiveProjectionSource,
    snapshot: &SignedAccountSnapshot,
    now_ms: u64,
) -> bool {
    if let Some(kol_user_id) = source.kol_user_id.as_deref() {
        for fill in snapshot.fills() {
            let normalized = match source_fill_from_signed(
                &source.trading_account_id,
                fill,
                snapshot.observed_at_ms(),
            ) {
                Ok(fill) => fill,
                Err(_) => return false,
            };
            if executor_store
                .record_source_fill_and_plan(kol_user_id, &normalized, now_ms)
                .await
                .is_err()
            {
                return false;
            }
        }
    }
    projection_store
        .persist(source, snapshot, now_ms)
        .await
        .is_ok()
}

fn is_current_worker(
    workers: &BTreeMap<String, ProjectionWorker>,
    credential_id: &str,
    worker_id: u64,
) -> bool {
    workers
        .get(credential_id)
        .is_some_and(|worker| worker.id == worker_id)
}

fn begin_projection_persistence(
    workers: &mut BTreeMap<String, ProjectionWorker>,
    credential_id: &str,
    worker_id: u64,
) -> bool {
    let Some(worker) = workers
        .get_mut(credential_id)
        .filter(|worker| worker.id == worker_id && !worker.persistence_in_flight)
    else {
        return false;
    };
    worker.persistence_in_flight = true;
    true
}

/// The blocking gateway worker waits for this settlement before it can publish another turn, so
/// one credential remains strictly ordered while unrelated credentials persist concurrently.
fn finish_projection_persistence(
    workers: &mut BTreeMap<String, ProjectionWorker>,
    credential_id: &str,
    worker_id: u64,
    healthy: bool,
) -> bool {
    let Some(worker) = workers
        .get_mut(credential_id)
        .filter(|worker| worker.id == worker_id && worker.persistence_in_flight)
    else {
        return false;
    };
    worker.persistence_in_flight = false;
    if !healthy && let Some(worker) = workers.remove(credential_id) {
        let _ = worker.stop.send(true);
    }
    true
}

fn same_subscription(left: &ActiveProjectionSource, right: &ActiveProjectionSource) -> bool {
    left.kol_user_id == right.kol_user_id
        && left.owner_user_id == right.owner_user_id
        && left.credential_id == right.credential_id
        && left.trading_account_id == right.trading_account_id
        && left.symbols == right.symbols
}

enum PrivatePollAction {
    Idle,
    StreamFill(BinancePrivateFillEvent),
    RefreshRecommended,
    SignedCorrection,
}

fn private_poll_action(event: Option<BinancePrivateAccountEvent>) -> PrivatePollAction {
    match event {
        Some(BinancePrivateAccountEvent::Fill(event)) => PrivatePollAction::StreamFill(event),
        Some(BinancePrivateAccountEvent::RefreshRecommended) => {
            PrivatePollAction::RefreshRecommended
        }
        Some(BinancePrivateAccountEvent::ReconcileRequired { .. }) => {
            PrivatePollAction::SignedCorrection
        }
        None => PrivatePollAction::Idle,
    }
}

struct PrivateFillBurst {
    events: Vec<BinancePrivateFillEvent>,
    signed_correction: bool,
    deferred: Option<BinancePrivateAccountEvent>,
}

fn collect_private_fill_burst<F, E>(
    first: BinancePrivateFillEvent,
    first_received_at: std::time::Instant,
    poll: F,
) -> PrivateFillBurst
where
    F: FnMut(std::time::Duration) -> Result<Option<BinancePrivateAccountEvent>, E>,
{
    collect_private_fill_burst_with_clock(first, first_received_at, poll, std::time::Instant::now)
}

fn collect_private_fill_burst_with_clock<F, E, N>(
    first: BinancePrivateFillEvent,
    first_received_at: std::time::Instant,
    mut poll: F,
    mut monotonic_now: N,
) -> PrivateFillBurst
where
    F: FnMut(std::time::Duration) -> Result<Option<BinancePrivateAccountEvent>, E>,
    N: FnMut() -> std::time::Instant,
{
    let generation = (first.stream_private_generation, first.private_generation);
    let deadline = first_received_at + PRIVATE_STREAM_FILL_BATCH_WINDOW;
    let mut events = Vec::with_capacity(PRIVATE_STREAM_FILL_BATCH_LIMIT);
    events.push(first);
    let mut signed_correction = false;
    let mut deferred = None;
    while events.len() < PRIVATE_STREAM_FILL_BATCH_LIMIT {
        let Some(remaining) = deadline.checked_duration_since(monotonic_now()) else {
            break;
        };
        if remaining.is_zero() {
            break;
        }
        match poll(remaining) {
            Ok(Some(BinancePrivateAccountEvent::Fill(event)))
                if (event.stream_private_generation, event.private_generation) == generation =>
            {
                events.push(event);
            }
            Ok(Some(event @ BinancePrivateAccountEvent::Fill(_))) => {
                deferred = Some(event);
                signed_correction = true;
                break;
            }
            Ok(Some(BinancePrivateAccountEvent::ReconcileRequired { .. })) | Err(_) => {
                signed_correction = true;
                break;
            }
            Ok(Some(BinancePrivateAccountEvent::RefreshRecommended)) => {}
            Ok(None) => break,
        }
    }
    PrivateFillBurst {
        events,
        signed_correction,
        deferred,
    }
}

fn spawn_projection_worker(
    worker_id: u64,
    source: ActiveProjectionSource,
    credentials: venue_gateway_binance::BinanceCredentials,
    stop: tokio::sync::watch::Receiver<bool>,
    reconcile: std::sync::mpsc::Receiver<()>,
    hot_dispatch: GridHotDispatchCache,
    sender: tokio::sync::mpsc::Sender<ProjectionMessage>,
) {
    let async_runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let credential_id = source.credential_id.clone();
        let result = (|| {
            let primary_symbol = source.symbols.first().cloned()?;
            let binding = GatewayBinding::new(
                VenueId::Binance,
                GatewayMode::Live,
                source.trading_account_id.clone(),
                primary_symbol,
            )
            .ok()?;
            let limits =
                BinanceTransportLimits::new(EXECUTOR_HTTP_TIMEOUT, EXECUTOR_MAX_RESPONSE_BYTES)
                    .ok()?;
            let mut gateway =
                venue_gateway_binance::BinanceAccountGateway::connect_with_credentials_for_symbols(
                    binding,
                    source.symbols.clone(),
                    credentials,
                    limits,
                )
                .ok()?;
            gateway.prime_private_stream().ok()?;
            hot_dispatch.invalidate_credential(&credential_id);
            sender
                .blocking_send(ProjectionMessage::Invalidate {
                    credential_id: credential_id.clone(),
                    worker_id,
                })
                .ok()?;
            let mut fills_cursor = source.previous_fills_cursor.clone();
            // Establish and enqueue the signed baseline before the first stream event. Messages
            // from this worker share one FIFO sender, so the supervisor cannot persist a stream
            // suffix against a projection row that it has not seen yet.
            let initial = gateway
                .signed_projection_snapshot(fills_cursor.clone())
                .ok()?;
            gateway.install_stream_projection(initial.clone()).ok()?;
            let mut baseline_fill_ids = initial
                .fills()
                .iter()
                .map(|fill| (fill.symbol.clone(), fill.fill_id.clone()))
                .collect::<std::collections::BTreeSet<_>>();
            fills_cursor = Some(initial.fills_cursor().to_owned());
            let (initial_completion, initial_settled) = std::sync::mpsc::sync_channel(1);
            sender
                .blocking_send(ProjectionMessage::Snapshot {
                    worker_id,
                    source: source.clone(),
                    snapshot: initial,
                    completion: initial_completion,
                })
                .ok()?;
            if !initial_settled.recv().ok()? {
                return None;
            }
            let mut refresh_at = std::time::Instant::now();
            let mut publish_at = std::time::Instant::now() + STREAM_PROJECTION_INTERVAL;
            let mut consecutive_snapshot_failures = 0_u32;
            let mut deferred_private_event = None;
            let mut pending_snapshot: Option<(
                std::sync::mpsc::Receiver<
                    Result<
                        venue_gateway_binance::BinanceCompletedProjection,
                        venue_gateway_binance::BinanceAccountGatewayError,
                    >,
                >,
                u64,
            )> = None;
            let mut fill_epoch = 0_u64;
            let mut recovering = false;
            let mut publish_deferred = 0_u32;
            while !*stop.borrow() {
                let forced_reconcile = match reconcile.try_recv() {
                    Ok(()) => true,
                    Err(std::sync::mpsc::TryRecvError::Empty) => false,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => return None,
                };
                let (action, action_received_at) = if forced_reconcile {
                    (
                        PrivatePollAction::SignedCorrection,
                        std::time::Instant::now(),
                    )
                } else if recovering && pending_snapshot.is_some() {
                    (PrivatePollAction::Idle, std::time::Instant::now())
                } else if let Some(event) = deferred_private_event.take() {
                    (private_poll_action(Some(event)), std::time::Instant::now())
                } else {
                    match gateway.poll_private_fill_timed() {
                        Ok(Some((received_at, event))) => {
                            (private_poll_action(Some(event)), received_at)
                        }
                        Ok(None) => (PrivatePollAction::Idle, std::time::Instant::now()),
                        Err(error) => {
                            tracing::warn!(target: "venue_control::grid_hot_path", error = %error, "Authenticated stream polling failed; requesting signed correction");
                            (
                                PrivatePollAction::SignedCorrection,
                                std::time::Instant::now(),
                            )
                        }
                    }
                };
                let (private_changed, signed_correction) = match action {
                    PrivatePollAction::StreamFill(event) => {
                        if baseline_fill_ids
                            .contains(&(event.fill.symbol.clone(), event.fill.fill_id.clone()))
                        {
                            continue;
                        }
                        fill_epoch = fill_epoch.saturating_add(1);
                        let mut burst =
                            collect_private_fill_burst(event, action_received_at, |remaining| {
                                gateway.poll_private_fill_with_budget(remaining)
                            });
                        burst.events.retain(|event| {
                            !baseline_fill_ids
                                .contains(&(event.fill.symbol.clone(), event.fill.fill_id.clone()))
                        });
                        deferred_private_event = burst.deferred;
                        if recovering {
                            // The signed cursor repairs all fills observed across a genuine gap.
                            // The new baseline includes these buffered gap events before rolling.
                            continue;
                        }
                        let (completion, settled) = std::sync::mpsc::sync_channel(1);
                        if sender
                            .blocking_send(ProjectionMessage::StreamFills {
                                worker_id,
                                source: source.clone(),
                                events: burst.events,
                                completion,
                            })
                            .is_err()
                        {
                            return None;
                        }
                        if !settled.recv().ok()? {
                            return None;
                        }
                        (true, burst.signed_correction)
                    }
                    PrivatePollAction::SignedCorrection => (true, true),
                    PrivatePollAction::RefreshRecommended => (true, false),
                    PrivatePollAction::Idle => (false, false),
                };
                if signed_correction {
                    recovering = true;
                    refresh_at = std::time::Instant::now();
                    hot_dispatch.invalidate_credential(&credential_id);
                    sender
                        .blocking_send(ProjectionMessage::Invalidate {
                            credential_id: credential_id.clone(),
                            worker_id,
                        })
                        .ok()?;
                }
                let ready = pending_snapshot
                    .as_ref()
                    .and_then(|(receiver, epoch)| match receiver.try_recv() {
                        Ok(result) => Some((result, *epoch)),
                        Err(std::sync::mpsc::TryRecvError::Empty) => None,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => Some((
                            Err(venue_gateway_binance::BinanceAccountGatewayError::Readback),
                            *epoch,
                        )),
                    });
                if let Some((result, started_epoch)) = ready {
                    pending_snapshot = None;
                    match result {
                        Ok(completed) if recovering || started_epoch == fill_epoch => {
                            let snapshot = completed.snapshot().clone();
                            let snapshot_for_install = snapshot.clone();
                            baseline_fill_ids = snapshot
                                .fills()
                                .iter()
                                .map(|fill| (fill.symbol.clone(), fill.fill_id.clone()))
                                .collect();
                            fills_cursor = Some(snapshot.fills_cursor().to_owned());
                            let (completion, settled) = std::sync::mpsc::sync_channel(1);
                            sender
                                .blocking_send(ProjectionMessage::Snapshot {
                                    worker_id,
                                    source: source.clone(),
                                    snapshot,
                                    completion,
                                })
                                .ok()?;
                            if !settled.recv().ok()? {
                                return None;
                            }
                            gateway.accept_projection_read(completed).ok()?;
                            gateway
                                .install_stream_projection(snapshot_for_install)
                                .ok()?;
                            recovering = false;
                            consecutive_snapshot_failures = 0;
                            publish_at = std::time::Instant::now() + STREAM_PROJECTION_INTERVAL;
                        }
                        Ok(_) => {
                            // Never replace a live post-fill projection with a REST collection
                            // which began before that fill. Re-read without blocking the stream.
                            refresh_at = std::time::Instant::now();
                        }
                        Err(_) => {
                            consecutive_snapshot_failures =
                                consecutive_snapshot_failures.saturating_add(1);
                            let delay = projection_retry_delay(consecutive_snapshot_failures);
                            refresh_at = std::time::Instant::now() + delay;
                        }
                    }
                }
                if recovering
                    && gateway.private_stream_recovery_ready()
                    && pending_snapshot.is_none()
                    && std::time::Instant::now() >= refresh_at
                {
                    let read = gateway.prepare_projection_read(fills_cursor.clone()).ok()?;
                    let (completed, receiver) = std::sync::mpsc::sync_channel(1);
                    async_runtime.spawn(async move {
                        let _ = completed.send(read.collect().await);
                    });
                    pending_snapshot = Some((receiver, fill_epoch));
                }
                if !recovering && !private_changed && std::time::Instant::now() >= publish_at {
                    if let Some(snapshot) = gateway.stream_projection_snapshot().map_err(|error| {
                        tracing::warn!(target: "venue_control::grid_hot_path", %error, "Authenticated account projection lost continuity; rebuilding baseline");
                    }).ok()? {
                        let observed = snapshot.observed_at_ms();
                        let next_cursor = snapshot.fills_cursor().to_owned();
                        let (completion, settled) = std::sync::mpsc::sync_channel(1);
                        sender
                            .blocking_send(ProjectionMessage::StreamSnapshot {
                                worker_id,
                                source: source.clone(),
                                snapshot,
                                completion,
                            })
                            .ok()?;
                        match settled.recv().ok()?.ok()? {
                            Some(true) => {
                                gateway.accept_stream_projection(observed);
                                fills_cursor = Some(next_cursor);
                                publish_deferred = 0;
                            }
                            Some(false) => {
                                publish_deferred = publish_deferred.saturating_add(1);
                            }
                            None => publish_deferred = 0,
                        }
                        if publish_deferred >= 10 {
                            tracing::warn!(target: "venue_control::grid_hot_path", "Quiescent stream order surface differs from the durable target; requesting signed correction");
                            recovering = true;
                            refresh_at = std::time::Instant::now();
                            hot_dispatch.invalidate_credential(&credential_id);
                            sender
                                .blocking_send(ProjectionMessage::Invalidate {
                                    credential_id: credential_id.clone(),
                                    worker_id,
                                })
                                .ok()?;
                        }
                    }
                    publish_at = std::time::Instant::now() + STREAM_PROJECTION_INTERVAL;
                }
                if !private_changed {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
            Some(())
        })();
        let _ = result;
        hot_dispatch.invalidate_credential(&credential_id);
        let _ = sender.blocking_send(ProjectionMessage::Stopped {
            credential_id,
            worker_id,
        });
    });
}

fn projection_retry_delay(consecutive_failures: u32) -> std::time::Duration {
    let shift = consecutive_failures
        .saturating_sub(1)
        .min(MAX_PROJECTION_RETRY_SHIFT);
    std::time::Duration::from_millis(250_u64 << shift)
}

fn now_ms() -> Result<u64, std::io::Error> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(std::io::Error::other)?;
    u64::try_from(elapsed.as_millis()).map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::{BTreeSet, VecDeque},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use venue_domain::domain::{FieldState, Fill, OrderSide, PositionSide, Price};

    fn source(credential_id: &str) -> ActiveProjectionSource {
        ActiveProjectionSource {
            kol_user_id: None,
            owner_user_id: "owner".to_owned(),
            credential_id: credential_id.to_owned(),
            trading_account_id: "account".to_owned(),
            symbols: BTreeSet::new(),
            previous_fills_cursor: None,
        }
    }

    fn stream_fill(fill_id: &str) -> Result<BinancePrivateFillEvent, Box<dyn std::error::Error>> {
        Ok(BinancePrivateFillEvent {
            stream_private_generation: 3,
            private_generation: 3,
            received_at_ms: 200,
            fill: Fill {
                fill_id: fill_id.to_owned(),
                execution_sequence: FieldState::Known(7),
                order_id: format!("order-{fill_id}"),
                symbol: "BTC/USDT".parse()?,
                side: OrderSide::Buy,
                position_side: FieldState::Known(PositionSide::Long),
                quantity: rust_decimal::Decimal::new(1, 3),
                price: Price::new(rust_decimal::Decimal::new(100_000, 0))?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(true),
                exchange_time_ms: Some(199),
            },
            client_order_id: FieldState::Known(format!("client-{fill_id}")),
            original_quantity: FieldState::Known(rust_decimal::Decimal::new(2, 3)),
            cumulative_filled_quantity: FieldState::Known(rust_decimal::Decimal::new(1, 3)),
            order_state: FieldState::Known(venue_domain::domain::OrderState::PartiallyFilled),
        })
    }

    #[test]
    fn one_private_fill_is_released_when_the_window_has_no_second_fill()
    -> Result<(), Box<dyn std::error::Error>> {
        let started_at = std::time::Instant::now();
        let burst = collect_private_fill_burst_with_clock(
            stream_fill("trade-1")?,
            started_at,
            |remaining| {
                assert_eq!(remaining, PRIVATE_STREAM_FILL_BATCH_WINDOW);
                Ok::<_, ()>(None)
            },
            || started_at,
        );
        assert_eq!(burst.events.len(), 1);
        assert_eq!(burst.events[0].fill.fill_id, "trade-1");
        assert!(!burst.signed_correction);
        assert!(burst.deferred.is_none());
        Ok(())
    }

    #[test]
    fn two_ready_private_fills_share_one_burst() -> Result<(), Box<dyn std::error::Error>> {
        let mut pending = VecDeque::from([
            Some(BinancePrivateAccountEvent::Fill(stream_fill("trade-2")?)),
            None,
        ]);
        let started_at = std::time::Instant::now();
        let burst = collect_private_fill_burst_with_clock(
            stream_fill("trade-1")?,
            started_at,
            |_| Ok::<_, ()>(pending.pop_front().flatten()),
            || started_at,
        );
        assert_eq!(
            burst
                .events
                .iter()
                .map(|event| event.fill.fill_id.as_str())
                .collect::<Vec<_>>(),
            ["trade-1", "trade-2"]
        );
        assert!(!burst.signed_correction);
        Ok(())
    }

    #[test]
    fn private_fill_burst_stops_at_five_without_consuming_a_sixth()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut pending = (2..=6)
            .map(|index| {
                stream_fill(&format!("trade-{index}")).map(BinancePrivateAccountEvent::Fill)
            })
            .collect::<Result<VecDeque<_>, _>>()?;
        let started_at = std::time::Instant::now();
        let burst = collect_private_fill_burst_with_clock(
            stream_fill("trade-1")?,
            started_at,
            |_| Ok::<_, ()>(pending.pop_front()),
            || started_at,
        );
        assert_eq!(burst.events.len(), PRIVATE_STREAM_FILL_BATCH_LIMIT);
        assert_eq!(
            pending.front(),
            Some(&BinancePrivateAccountEvent::Fill(stream_fill("trade-6")?))
        );
        Ok(())
    }

    #[test]
    fn expired_first_fill_window_never_polls_for_a_second_fill()
    -> Result<(), Box<dyn std::error::Error>> {
        let started_at = std::time::Instant::now();
        let burst = collect_private_fill_burst_with_clock(
            stream_fill("trade-1")?,
            started_at,
            |_| -> Result<Option<BinancePrivateAccountEvent>, ()> {
                panic!("expired burst must not poll")
            },
            || started_at + PRIVATE_STREAM_FILL_BATCH_WINDOW,
        );
        assert_eq!(burst.events.len(), 1);
        Ok(())
    }

    #[test]
    fn reconcile_and_generation_boundaries_are_preserved_after_the_current_burst()
    -> Result<(), Box<dyn std::error::Error>> {
        let started_at = std::time::Instant::now();
        let burst = collect_private_fill_burst_with_clock(
            stream_fill("trade-1")?,
            started_at,
            |_| {
                Ok::<_, ()>(Some(BinancePrivateAccountEvent::ReconcileRequired {
                    stream_private_generation: 3,
                    private_generation: 3,
                    received_at_ms: 201,
                }))
            },
            || started_at,
        );
        assert_eq!(burst.events.len(), 1);
        assert!(burst.signed_correction);
        assert!(burst.deferred.is_none());

        let mut next_generation = stream_fill("trade-2")?;
        next_generation.stream_private_generation = 4;
        next_generation.private_generation = 4;
        let started_at = std::time::Instant::now();
        let burst = collect_private_fill_burst_with_clock(
            stream_fill("trade-1")?,
            started_at,
            |_| {
                Ok::<_, ()>(Some(BinancePrivateAccountEvent::Fill(
                    next_generation.clone(),
                )))
            },
            || started_at,
        );
        assert!(burst.signed_correction);
        assert!(matches!(
            burst.deferred,
            Some(BinancePrivateAccountEvent::Fill(event)) if event.fill.fill_id == "trade-2"
        ));
        assert!(matches!(
            private_poll_action(Some(BinancePrivateAccountEvent::ReconcileRequired {
                stream_private_generation: 3,
                private_generation: 3,
                received_at_ms: 201,
            })),
            PrivatePollAction::SignedCorrection
        ));
        assert!(matches!(private_poll_action(None), PrivatePollAction::Idle));
        Ok(())
    }

    #[test]
    fn executor_wakes_for_committed_commands_even_if_a_later_kol_fill_retires_the_worker() {
        assert!(!StreamFillBatchOutcome::FailedBeforePersistence.should_wake_executor());
        assert!(!StreamFillBatchOutcome::FailedBeforePersistence.worker_healthy());
        let duplicate = StreamFillBatchOutcome::Persisted {
            planned_commands: false,
            worker_healthy: true,
        };
        assert!(!duplicate.should_wake_executor());
        assert!(duplicate.worker_healthy());
        let partial_failure = StreamFillBatchOutcome::Persisted {
            planned_commands: true,
            worker_healthy: false,
        };
        assert!(partial_failure.should_wake_executor());
        assert!(!partial_failure.worker_healthy());
    }

    #[tokio::test]
    async fn grid_batch_must_settle_before_kol_planning_starts()
    -> Result<(), Box<dyn std::error::Error>> {
        let events = vec![stream_fill("trade-1")?, stream_fill("trade-2")?];
        let (grid_tx, mut grid_rx) = tokio::sync::mpsc::channel(1);
        let kol_calls = Arc::new(AtomicUsize::new(0));
        let observed_calls = Arc::clone(&kol_calls);
        let dispatch = tokio::spawn(async move {
            dispatch_persisted_stream_batch(&grid_tx, &source("credential"), &events, move |_| {
                let observed_calls = Arc::clone(&observed_calls);
                async move {
                    observed_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(false)
                }
            })
            .await
        });
        let Some(GridPrivateStreamSignal::FillBatch {
            events, completion, ..
        }) = grid_rx.recv().await
        else {
            return Err("missing Grid fill batch".into());
        };
        assert_eq!(
            events
                .iter()
                .map(|event| event.fill.fill_id.as_str())
                .collect::<Vec<_>>(),
            ["trade-1", "trade-2"]
        );
        assert_eq!(kol_calls.load(Ordering::SeqCst), 0);
        completion
            .send(true)
            .map_err(|_| "Grid completion dropped")?;
        let outcome = dispatch.await?;
        assert!(outcome.worker_healthy);
        assert!(!outcome.planned_commands);
        assert_eq!(kol_calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn later_kol_failure_preserves_prior_command_wake_and_retires_the_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        let events = vec![stream_fill("trade-1")?, stream_fill("trade-2")?];
        let (grid_tx, mut grid_rx) = tokio::sync::mpsc::channel(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let dispatch = tokio::spawn(async move {
            dispatch_persisted_stream_batch(&grid_tx, &source("credential"), &events, move |_| {
                let call = observed.fetch_add(1, Ordering::SeqCst);
                async move { if call == 0 { Ok(true) } else { Err(()) } }
            })
            .await
        });
        let Some(GridPrivateStreamSignal::FillBatch { completion, .. }) = grid_rx.recv().await
        else {
            return Err("missing Grid fill batch".into());
        };
        completion
            .send(true)
            .map_err(|_| "Grid completion dropped")?;
        let outcome = dispatch.await?;
        assert!(outcome.planned_commands);
        assert!(!outcome.worker_healthy);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[tokio::test]
    async fn rejected_grid_batch_never_starts_kol_planning()
    -> Result<(), Box<dyn std::error::Error>> {
        let events = vec![stream_fill("trade-1")?];
        let (grid_tx, mut grid_rx) = tokio::sync::mpsc::channel(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let dispatch = tokio::spawn(async move {
            dispatch_persisted_stream_batch(&grid_tx, &source("credential"), &events, move |_| {
                let observed = Arc::clone(&observed);
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(true)
                }
            })
            .await
        });
        let Some(GridPrivateStreamSignal::FillBatch { completion, .. }) = grid_rx.recv().await
        else {
            return Err("missing Grid fill batch".into());
        };
        completion
            .send(false)
            .map_err(|_| "Grid completion dropped")?;
        let outcome = dispatch.await?;
        assert!(!outcome.worker_healthy);
        assert!(!outcome.planned_commands);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn full_grid_channel_is_covered_by_the_same_bounded_settlement_timeout()
    -> Result<(), Box<dyn std::error::Error>> {
        let events = vec![stream_fill("trade-1")?];
        let (grid_tx, mut grid_rx) = tokio::sync::mpsc::channel(1);
        grid_tx
            .send(GridPrivateStreamSignal::Invalidate {
                credential_id: "occupied".into(),
            })
            .await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let outcome = dispatch_persisted_stream_batch_with_timeout(
            &grid_tx,
            &source("credential"),
            &events,
            std::time::Duration::from_millis(1),
            move |_| {
                let observed = Arc::clone(&observed);
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(true)
                }
            },
        )
        .await;
        assert!(!outcome.worker_healthy);
        assert!(!outcome.planned_commands);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            grid_rx.recv().await,
            Some(GridPrivateStreamSignal::Invalidate { .. })
        ));
        Ok(())
    }

    #[test]
    fn stopped_message_only_matches_the_worker_that_created_it() {
        let (stop, _) = tokio::sync::watch::channel(false);
        let (reconcile, _) = std::sync::mpsc::sync_channel(1);
        let mut workers = BTreeMap::new();
        workers.insert(
            "credential".to_owned(),
            ProjectionWorker {
                id: 2,
                source: source("credential"),
                stop,
                reconcile,
                persistence_in_flight: false,
            },
        );

        assert!(!is_current_worker(&workers, "credential", 1));
        assert!(is_current_worker(&workers, "credential", 2));
        assert!(!is_current_worker(&workers, "other", 2));
    }

    #[test]
    fn projection_persistence_is_serial_per_worker_and_failure_retires_it() {
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let (reconcile, _) = std::sync::mpsc::sync_channel(1);
        let mut workers = BTreeMap::new();
        workers.insert(
            "credential".to_owned(),
            ProjectionWorker {
                id: 2,
                source: source("credential"),
                stop,
                reconcile,
                persistence_in_flight: false,
            },
        );

        assert!(!begin_projection_persistence(&mut workers, "credential", 1));
        assert!(begin_projection_persistence(&mut workers, "credential", 2));
        assert!(!begin_projection_persistence(&mut workers, "credential", 2));
        assert!(finish_projection_persistence(
            &mut workers,
            "credential",
            2,
            true
        ));
        assert!(workers.contains_key("credential"));
        assert!(begin_projection_persistence(&mut workers, "credential", 2));
        assert!(finish_projection_persistence(
            &mut workers,
            "credential",
            2,
            false
        ));
        assert!(!workers.contains_key("credential"));
        assert!(*stopped.borrow());
    }

    #[test]
    fn kol_role_change_rebuilds_the_projection_worker() {
        let ordinary = source("credential");
        let mut kol = ordinary.clone();
        kol.kol_user_id = Some("kol".to_owned());

        assert!(!same_subscription(&ordinary, &kol));
        assert!(same_subscription(&kol, &kol));
    }

    #[test]
    fn projection_snapshot_failures_retry_before_reconnecting_the_worker() {
        assert_eq!(
            projection_retry_delay(1),
            std::time::Duration::from_millis(250)
        );
        assert_eq!(projection_retry_delay(4), std::time::Duration::from_secs(2));
        assert_eq!(projection_retry_delay(5), std::time::Duration::from_secs(4));
        assert_eq!(
            projection_retry_delay(100),
            std::time::Duration::from_secs(8)
        );
    }
}
