use super::{AccountError, error};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use venue_control_protocol::accounts::AccountErrorCode;
use venue_execution::SignedAccountSnapshot;

type ReadResult = Result<SignedAccountSnapshot, AccountError>;
type Entry = Arc<Mutex<ReadState>>;
const CAPACITY: usize = 232;
const REUSE: Duration = Duration::from_millis(500);

#[derive(Default)]
struct ReadState {
    result: Option<(Instant, ReadResult)>,
    running: Option<tokio::task::JoinHandle<ReadResult>>,
}

/// Account-scoped display reads, not an execution or permission cache. No credentials are stored.
#[derive(Default)]
pub(super) struct ProjectionReadCache {
    entries: Mutex<BTreeMap<String, (Instant, Entry)>>,
}

impl ProjectionReadCache {
    pub(super) async fn read<F>(&self, key: String, fetch: F) -> ReadResult
    where
        F: std::future::Future<Output = ReadResult> + Send + 'static,
    {
        let entry = {
            let mut entries = self.entries.lock().await;
            entries.retain(|_, (time, entry)| {
                time.elapsed() < Duration::from_secs(60) || Arc::strong_count(entry) > 1
            });
            if !entries.contains_key(&key) && entries.len() >= CAPACITY {
                return Err(error(AccountErrorCode::Unavailable));
            }
            let (time, entry) = entries
                .entry(key)
                .or_insert_with(|| (Instant::now(), Arc::default()));
            *time = Instant::now();
            entry.clone()
        };
        let mut state = entry.lock().await;
        if let Some((time, value)) = &state.result
            && time.elapsed() < REUSE
        {
            return value.clone();
        }
        // Keep an in-flight read across an SSE timeout/reconnect. Dropping a waiter must not
        // launch another signed query while its blocking adapter is still finishing.
        let running = state.running.get_or_insert_with(|| tokio::spawn(fetch));
        let result = running
            .await
            .unwrap_or_else(|_| Err(error(AccountErrorCode::Unavailable)));
        state.running = None;
        state.result = Some((Instant::now(), result.clone()));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn concurrent_clients_and_reconnects_share_one_read() {
        let cache = Arc::new(ProjectionReadCache::default());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (send, receive) = tokio::sync::oneshot::channel();
        let first_cache = cache.clone();
        let first_calls = calls.clone();
        let waiter = tokio::spawn(async move {
            first_cache
                .read("owner/account".into(), async move {
                    first_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let _ = send.send(());
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    Err(error(AccountErrorCode::Unavailable))
                })
                .await
        });
        assert!(receive.await.is_ok());
        waiter.abort();
        let second_calls = calls.clone();
        let result = cache
            .read("owner/account".into(), async move {
                second_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(error(AccountErrorCode::Conflict))
            })
            .await;
        assert_eq!(
            result.err().map(|e| e.code),
            Some(AccountErrorCode::Unavailable)
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
