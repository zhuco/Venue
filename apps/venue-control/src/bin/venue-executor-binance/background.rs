use super::{PROJECTION_PERSISTENCE_TIMEOUT, ProjectionMessage};
use std::future::Future;
use tokio::{sync::mpsc, task::JoinSet};

const CONNECTIONS_PER_POOL: u32 = 4;

pub(super) struct ExecutorDatabase {
    pub command: sqlx::PgPool,
    pub background: sqlx::PgPool,
}

pub(super) async fn connect_database(url: &str) -> Result<ExecutorDatabase, sqlx::Error> {
    // The combined budget remains eight. Background work cannot exhaust the permits reserved
    // for command claims, ownership/permission reads, dispatch transitions and reconciliation.
    let command = sqlx::postgres::PgPoolOptions::new()
        .max_connections(CONNECTIONS_PER_POOL)
        .connect(url)
        .await?;
    let background = sqlx::postgres::PgPoolOptions::new()
        .max_connections(CONNECTIONS_PER_POOL)
        .connect(url)
        .await?;
    Ok(ExecutorDatabase {
        command,
        background,
    })
}

/// Admission is owned by begin_projection_persistence: one outstanding turn per active
/// credential. The gateway waits for settlement, so slow SQL cannot build a snapshot backlog.
pub(super) fn spawn_stream_snapshot(
    tasks: &mut JoinSet<()>,
    messages: mpsc::Sender<ProjectionMessage>,
    credential_id: String,
    worker_id: u64,
    completion: std::sync::mpsc::SyncSender<Result<Option<bool>, ()>>,
    persist: impl Future<Output = Result<Option<bool>, ()>> + Send + 'static,
) {
    tasks.spawn(async move {
        let result = tokio::time::timeout(PROJECTION_PERSISTENCE_TIMEOUT, persist)
            .await
            .unwrap_or(Err(()));
        let _ = messages
            .send(ProjectionMessage::StreamSnapshotSettled {
                credential_id,
                worker_id,
                result,
                completion,
            })
            .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn saturated_background_database_keeps_command_connections_available()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(url) = std::env::var("VENUE_CONTROL_TEST_DATABASE_URL") else {
            if std::env::var("VENUE_CONTROL_POSTGRES_REQUIRED").as_deref() == Ok("1") {
                return Err("isolated PostgreSQL test database is required".into());
            }
            eprintln!("SKIP: background pool isolation requires VENUE_CONTROL_TEST_DATABASE_URL");
            return Ok(());
        };
        let database = connect_database(&url).await?;
        let mut occupied = Vec::new();
        for _ in 0..CONNECTIONS_PER_POOL {
            occupied.push(database.background.acquire().await?);
        }
        assert!(database.background.try_acquire().is_none());
        let value: i32 = tokio::time::timeout(
            Duration::from_secs(1),
            sqlx::query_scalar("SELECT 1").fetch_one(&database.command),
        )
        .await??;
        assert_eq!(value, 1);
        drop(occupied);
        database.background.close().await;
        database.command.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn slow_snapshot_does_not_delay_another_accounts_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let (messages, mut received) = mpsc::channel(2);
        let mut tasks = JoinSet::new();
        let (release, wait) = tokio::sync::oneshot::channel();
        let (slow, _slow_result) = std::sync::mpsc::sync_channel(1);
        spawn_stream_snapshot(
            &mut tasks,
            messages.clone(),
            "slow".into(),
            1,
            slow,
            async {
                wait.await.map_err(|_| ())?;
                Ok(Some(true))
            },
        );
        let (fast, _fast_result) = std::sync::mpsc::sync_channel(1);
        spawn_stream_snapshot(&mut tasks, messages, "fast".into(), 2, fast, async {
            Ok(Some(true))
        });
        let first = tokio::time::timeout(Duration::from_secs(1), received.recv()).await?;
        assert!(
            matches!(first, Some(ProjectionMessage::StreamSnapshotSettled {
            credential_id, worker_id: 2, result: Ok(Some(true)), ..
        }) if credential_id == "fast")
        );
        release.send(()).map_err(|_| "slow task dropped")?;
        let second = tokio::time::timeout(Duration::from_secs(1), received.recv()).await?;
        assert!(matches!(
            second,
            Some(ProjectionMessage::StreamSnapshotSettled {
                worker_id: 1,
                result: Ok(Some(true)),
                ..
            })
        ));
        while let Some(result) = tasks.join_next().await {
            result?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn stalled_snapshot_fails_closed_and_shutdown_drops_pending_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let (messages, mut received) = mpsc::channel(1);
        let mut tasks = JoinSet::new();
        let (completion, _response) = std::sync::mpsc::sync_channel(1);
        spawn_stream_snapshot(
            &mut tasks,
            messages.clone(),
            "timeout".into(),
            1,
            completion,
            std::future::pending(),
        );
        let message = tokio::time::timeout(
            PROJECTION_PERSISTENCE_TIMEOUT + Duration::from_secs(1),
            received.recv(),
        )
        .await?;
        assert!(matches!(
            message,
            Some(ProjectionMessage::StreamSnapshotSettled {
                result: Err(()),
                ..
            })
        ));
        let (completion, response) = std::sync::mpsc::sync_channel(1);
        spawn_stream_snapshot(
            &mut tasks,
            messages,
            "shutdown".into(),
            2,
            completion,
            std::future::pending(),
        );
        tasks.shutdown().await;
        assert!(matches!(
            response.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Disconnected)
        ));
        assert!(received.try_recv().is_err());
        Ok(())
    }
}
