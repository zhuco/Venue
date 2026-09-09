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
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let now = now_ms().map_err(|_| ())?;
        let result = time::timeout(Duration::from_secs(5), async {
            let principal = accounts.authenticate(token.expose(), now).await?;
            accounts
                .terminal_account_projection(&principal, request.clone(), now)
                .await
        })
        .await;
        let projection = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) if !opened => return account_error(stream, error.code).await,
            Err(_) if !opened => return account_error(stream, AccountErrorCode::Unavailable).await,
            _ => return Err(()),
        };
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
        let next_read = time::Instant::now() + Duration::from_millis(500);
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
