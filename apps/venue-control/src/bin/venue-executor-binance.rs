//! Singleton process entrypoint for the Binance KOL executor.
//!
//! This binary assembles the singleton's restricted PostgreSQL and master-key boundaries. It
//! never reads Binance API secrets from environment variables.

use sqlx::postgres::PgPoolOptions;
use std::collections::BTreeMap;
use venue_control::{
    BinanceExecutorSingleton,
    accounts::CredentialCipher,
    executor_config::ExecutorLaunchConfig,
    executor_exchange::BinanceExecutionRouter,
    executor_runtime::{BinanceExecutorRuntime, ExecutorCredentials},
    executor_secret::ExecutorSecretProvider,
    executor_store::PgExecutorStore,
    kol_private_source::{BinanceKolPrivateSource, persist_private_event_for_account},
    private_projection::{ActiveProjectionSource, BinancePrivateProjectionStore},
};
use venue_execution::SignedAccountSnapshot;
use venue_gateway_binance::{BinanceTransportLimits, GatewayBinding, GatewayMode, VenueId};

const EXECUTOR_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const EXECUTOR_MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const PROJECTION_DISCOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);
const PROJECTION_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_CONSECUTIVE_PROJECTION_FAILURES: u32 = 5;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let launch = ExecutorLaunchConfig::from_environment()?;
    let singleton = BinanceExecutorSingleton::acquire(&launch.database_url).await?;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&launch.database_url)
        .await?;
    let store = PgExecutorStore::new(pool.clone());
    let projection_store = BinancePrivateProjectionStore::new(pool.clone());
    let secrets = ExecutorSecretProvider::new(pool, CredentialCipher::from_environment()?);
    let exchange = BinanceExecutionRouter::new(BinanceTransportLimits::new(
        EXECUTOR_HTTP_TIMEOUT,
        EXECUTOR_MAX_RESPONSE_BYTES,
    )?);
    let mut runtime = BinanceExecutorRuntime::new(store.clone(), exchange, secrets.clone());
    // Promotion is a signed, fail-closed activation baseline; recovery then fences all
    // nonterminal commands before this process establishes any KOL event source.
    runtime.recover_once().await?;
    let source_specs = store.active_kol_private_sources(now_ms()?).await?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (source_tx, mut source_rx) = tokio::sync::mpsc::channel(64);
    for source in source_specs {
        let credentials = secrets
            .credentials(&source.credential_id, &source.kol_user_id)
            .await?;
        let primary_symbol = source
            .symbols
            .first()
            .cloned()
            .ok_or("active KOL source has no symbol")?;
        let binding = GatewayBinding::new(
            VenueId::Binance,
            GatewayMode::Live,
            source.leader_trading_account_id.clone(),
            primary_symbol,
        )?;
        let limits =
            BinanceTransportLimits::new(EXECUTOR_HTTP_TIMEOUT, EXECUTOR_MAX_RESPONSE_BYTES)?;
        let kol_user_id = source.kol_user_id.clone();
        let leader_account = source.leader_trading_account_id.clone();
        let source_kol_user_id = kol_user_id.clone();
        let mut private_source = tokio::task::spawn_blocking(move || {
            let symbols = source.symbols.into_iter().collect();
            let mut private_source = BinanceKolPrivateSource::connect(
                source_kol_user_id,
                binding,
                symbols,
                credentials,
                limits,
            )?;
            private_source.prime()?;
            Ok::<_, venue_gateway_binance::BinanceAccountGatewayError>(private_source)
        })
        .await??;
        let shutdown = shutdown_rx.clone();
        let sender = source_tx.clone();
        tokio::task::spawn_blocking(move || {
            while !*shutdown.borrow() {
                match private_source.poll() {
                    Ok(Some(event)) => {
                        let events = match event {
                            venue_gateway_binance::BinancePrivateAccountEvent::ReconcileRequired { .. } => {
                                match private_source.reconcile() {
                                    Ok(events) => events,
                                    Err(_) => {
                                        std::thread::sleep(std::time::Duration::from_millis(25));
                                        continue;
                                    }
                                }
                            }
                            event => vec![event],
                        };
                        for event in events {
                            if sender
                                .blocking_send((kol_user_id.clone(), leader_account.clone(), event))
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    Ok(None) => std::thread::sleep(std::time::Duration::from_millis(25)),
                    Err(_) => {
                        if let Ok(events) = private_source.reconcile() {
                            for event in events {
                                if sender
                                    .blocking_send((
                                        kol_user_id.clone(),
                                        leader_account.clone(),
                                        event,
                                    ))
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        std::thread::sleep(std::time::Duration::from_millis(25));
                    }
                }
            }
        });
    }
    drop(source_tx);
    tokio::spawn(run_projection_supervisor(
        projection_store,
        secrets.clone(),
        shutdown_rx.clone(),
    ));
    let source_store = store.clone();
    let mut source_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = source_shutdown.changed() => {
                    if changed.is_err() || *source_shutdown.borrow() { return; }
                }
                message = source_rx.recv() => match message {
                    Some((kol_user_id, leader_account, event)) => {
                        if let Ok(now) = now_ms() {
                            let _ = persist_private_event_for_account(&source_store, &kol_user_id, &leader_account, event, now).await;
                        }
                    }
                    None => return,
                }
            }
        }
    });
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
    singleton.release().await?;
    result?;
    Ok(())
}

enum ProjectionMessage {
    Snapshot {
        worker_id: u64,
        source: ActiveProjectionSource,
        snapshot: SignedAccountSnapshot,
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
}

async fn run_projection_supervisor(
    projection_store: BinancePrivateProjectionStore,
    secrets: ExecutorSecretProvider,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let (message_tx, mut message_rx) = tokio::sync::mpsc::channel(32);
    let mut workers = BTreeMap::<String, ProjectionWorker>::new();
    let mut next_worker_id = 0_u64;
    let mut discovery = tokio::time::interval(PROJECTION_DISCOVERY_INTERVAL);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    for (_, worker) in workers { let _ = worker.stop.send(true); }
                    return;
                }
            }
            message = message_rx.recv() => match message {
                Some(ProjectionMessage::Snapshot { worker_id, source, snapshot }) => {
                    if is_current_worker(&workers, &source.credential_id, worker_id)
                        && let Ok(now) = now_ms()
                    {
                        let _ = projection_store.persist(&source, &snapshot, now).await;
                    }
                }
                Some(ProjectionMessage::Stopped { credential_id, worker_id }) => {
                    if is_current_worker(&workers, &credential_id, worker_id) {
                        workers.remove(&credential_id);
                    }
                }
                None => return,
            },
            _ = discovery.tick() => {
                let Ok(now) = now_ms() else { continue; };
                let Ok(active) = projection_store.active_sources(now).await else { continue; };
                let active_by_id = active.into_iter().map(|source| (source.credential_id.clone(), source)).collect::<BTreeMap<_, _>>();
                let stale = workers.keys().filter(|id| !active_by_id.contains_key(*id)).cloned().collect::<Vec<_>>();
                for id in stale {
                    if let Some(worker) = workers.remove(&id) { let _ = worker.stop.send(true); }
                }
                for (credential_id, source) in active_by_id {
                    if workers.get(&credential_id).is_some_and(|worker| same_subscription(&worker.source, &source)) { continue; }
                    if let Some(worker) = workers.remove(&credential_id) { let _ = worker.stop.send(true); }
                    let Ok(credentials) = secrets.credentials(&source.credential_id, &source.owner_user_id).await else { continue; };
                    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
                    next_worker_id = next_worker_id.saturating_add(1);
                    let worker_id = next_worker_id;
                    workers.insert(credential_id, ProjectionWorker { id: worker_id, source: source.clone(), stop: stop_tx });
                    spawn_projection_worker(worker_id, source, credentials, stop_rx, message_tx.clone());
                }
            }
        }
    }
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

fn same_subscription(left: &ActiveProjectionSource, right: &ActiveProjectionSource) -> bool {
    left.owner_user_id == right.owner_user_id
        && left.credential_id == right.credential_id
        && left.trading_account_id == right.trading_account_id
        && left.symbols == right.symbols
}

fn spawn_projection_worker(
    worker_id: u64,
    source: ActiveProjectionSource,
    credentials: venue_gateway_binance::BinanceCredentials,
    stop: tokio::sync::watch::Receiver<bool>,
    sender: tokio::sync::mpsc::Sender<ProjectionMessage>,
) {
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
            let _ = gateway.prime_private_stream();
            let mut fills_cursor = source.previous_fills_cursor.clone();
            let mut refresh_at = std::time::Instant::now();
            let mut consecutive_snapshot_failures = 0_u32;
            while !*stop.borrow() {
                let private_changed = match gateway.poll_private_fill() {
                    Ok(Some(_)) | Err(_) => true,
                    Ok(None) => false,
                };
                if (private_changed && consecutive_snapshot_failures == 0)
                    || std::time::Instant::now() >= refresh_at
                {
                    let snapshot = match gateway.signed_projection_snapshot(fills_cursor.clone()) {
                        Ok(snapshot) => snapshot,
                        Err(_) => {
                            consecutive_snapshot_failures =
                                consecutive_snapshot_failures.saturating_add(1);
                            let Some(delay) = projection_retry_delay(consecutive_snapshot_failures)
                            else {
                                return None;
                            };
                            refresh_at = std::time::Instant::now() + delay;
                            continue;
                        }
                    };
                    consecutive_snapshot_failures = 0;
                    fills_cursor = Some(snapshot.fills_cursor().to_owned());
                    if sender
                        .blocking_send(ProjectionMessage::Snapshot {
                            worker_id,
                            source: source.clone(),
                            snapshot,
                        })
                        .is_err()
                    {
                        return None;
                    }
                    refresh_at = std::time::Instant::now() + PROJECTION_POLL_INTERVAL;
                }
                if !private_changed {
                    std::thread::sleep(std::time::Duration::from_millis(25));
                }
            }
            Some(())
        })();
        let _ = result;
        let _ = sender.blocking_send(ProjectionMessage::Stopped {
            credential_id,
            worker_id,
        });
    });
}

fn projection_retry_delay(consecutive_failures: u32) -> Option<std::time::Duration> {
    if consecutive_failures >= MAX_CONSECUTIVE_PROJECTION_FAILURES {
        return None;
    }
    let shift = consecutive_failures.saturating_sub(1).min(4);
    Some(std::time::Duration::from_millis(250_u64 << shift))
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
    use std::collections::BTreeSet;

    fn source(credential_id: &str) -> ActiveProjectionSource {
        ActiveProjectionSource {
            owner_user_id: "owner".to_owned(),
            credential_id: credential_id.to_owned(),
            trading_account_id: "account".to_owned(),
            symbols: BTreeSet::new(),
            previous_fills_cursor: None,
        }
    }

    #[test]
    fn stopped_message_only_matches_the_worker_that_created_it() {
        let (stop, _) = tokio::sync::watch::channel(false);
        let mut workers = BTreeMap::new();
        workers.insert(
            "credential".to_owned(),
            ProjectionWorker {
                id: 2,
                source: source("credential"),
                stop,
            },
        );

        assert!(!is_current_worker(&workers, "credential", 1));
        assert!(is_current_worker(&workers, "credential", 2));
        assert!(!is_current_worker(&workers, "other", 2));
    }

    #[test]
    fn projection_snapshot_failures_retry_before_reconnecting_the_worker() {
        assert_eq!(
            projection_retry_delay(1),
            Some(std::time::Duration::from_millis(250))
        );
        assert_eq!(
            projection_retry_delay(4),
            Some(std::time::Duration::from_secs(2))
        );
        assert_eq!(projection_retry_delay(5), None);
    }
}
