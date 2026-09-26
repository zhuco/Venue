use super::*;
use venue_control_protocol::kol::TerminalProjectionRequest;
use venue_control_protocol::terminal_account_stream::TerminalAccountStreamEvent;

pub(super) async fn serve<R>(
    stream: &mut TcpStream,
    state: &HttpState<R>,
    accounts: &AccountService,
    token: SecretValue,
    request: TerminalProjectionRequest,
    compact: bool,
    asset_clock: bool,
) -> Result<(), ()>
where
    R: ControlRepository + 'static,
{
    if request.validate().is_err() {
        return account_error(stream, AccountErrorCode::InvalidInput).await;
    }
    let mut changes = accounts.projection_changes();
    let mut shutdown = state.shutdown.clone();
    let mut fallback = time::interval(Duration::from_secs(1));
    fallback.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut opened = false;
    let mut previous = None;
    let mut next_history_read = time::Instant::now();
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let now = now_ms().map_err(|_| ())?;
        let refresh_history = time::Instant::now() >= next_history_read;
        let result = time::timeout(Duration::from_secs(5), async {
            let principal = accounts.authenticate(token.expose(), now).await?;
            accounts
                .terminal_account_projection_update(
                    &principal,
                    request.clone(),
                    now,
                    if refresh_history {
                        None
                    } else {
                        previous.as_ref()
                    },
                )
                .await
        })
        .await;
        let (mut projection, persisted_projection) = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) if !opened => return account_error(stream, error.code).await,
            Err(_) if !opened => return account_error(stream, AccountErrorCode::Unavailable).await,
            _ => return Err(()),
        };
        if !asset_clock && let Some(value) = &mut projection {
            value.balance_observed_ms = None;
        }
        let body = if compact {
            serde_json::to_string(&TerminalAccountStreamEvent::between(
                previous.as_ref(),
                projection.as_ref(),
            ))
        } else {
            serde_json::to_string(&projection)
        }
        .map_err(|_| ())?;
        if body.len() > 4 * 1024 * 1024 {
            return Err(());
        }
        if !opened {
            write_sse_headers(stream).await.map_err(|_| ())?;
            opened = true;
        }
        // Each connection starts with a full owner-checked snapshot. Unchanged history
        // must not queue hundreds of kilobytes ahead of current orders and positions.
        let frame = format!("event: terminal-account\ndata: {body}\n\n");
        write_sse(stream, frame.as_bytes(), state.config.request_timeout)
            .await
            .map_err(|_| ())?;
        previous = projection;
        if refresh_history {
            next_history_read = time::Instant::now() + Duration::from_secs(5);
        }
        let next_read = time::Instant::now() + coalesce_interval(persisted_projection);
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = fallback.tick() => {},
            result = changes.changed() => if result.is_err() { return Err(()); },
        }
        // Read the latest row after coalescing; global account notifications must not
        // turn a slow connection into an ever-growing queue of obsolete snapshots.
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = time::sleep_until(next_read) => {},
        }
    }
}

fn coalesce_interval(persisted_projection: bool) -> Duration {
    // Other venues may perform signed network reads; their cadence must not accelerate.
    Duration::from_millis(if persisted_projection { 33 } else { 500 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_wake_is_limited_to_persisted_account_facts() {
        assert_eq!(coalesce_interval(true), Duration::from_millis(33));
        assert_eq!(coalesce_interval(false), Duration::from_millis(500));
    }
}
