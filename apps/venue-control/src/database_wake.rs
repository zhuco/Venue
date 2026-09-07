//! Loss-tolerant PostgreSQL notifications; never a replacement for durable discovery or ownership.
use sqlx::{PgPool, postgres::PgListener};
use std::time::Duration;
use tokio::sync::watch;

pub fn listen(pool: PgPool, channel: &'static str) -> watch::Receiver<()> {
    let (sender, receiver) = watch::channel(());
    tokio::spawn(async move {
        loop {
            let connected = async {
                let mut listener = PgListener::connect_with(&pool).await?;
                listener.listen(channel).await?;
                Ok::<_, sqlx::Error>(listener)
            };
            let mut listener = tokio::select! {
                _ = sender.closed() => return,
                result = connected => match result {
                    Ok(listener) => listener,
                    Err(_) => {
                        tokio::select! {
                            _ = sender.closed() => return,
                            _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                        }
                    }
                }
            };
            // Force a read after LISTEN commits, including on reconnect, to close the subscribe race.
            sender.send_replace(());
            loop {
                tokio::select! {
                    _ = sender.closed() => return,
                    result = listener.try_recv() => match result {
                        Ok(_) => { sender.send_replace(()); }
                        Err(_) => break,
                    }
                }
            }
        }
    });
    receiver
}
