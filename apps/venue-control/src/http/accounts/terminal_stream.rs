use super::*;
use venue_control_protocol::kol::TerminalProjectionRequest;

pub(super) async fn serve<R>(
    stream: &mut TcpStream,
    state: &HttpState<R>,
    accounts: &AccountService,
    token: SecretValue,
    request: TerminalProjectionRequest,
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
        let body = serde_json::to_string(&projection).map_err(|_| ())?;
        if body.len() > 4 * 1024 * 1024 {
            return Err(());
        }
        if !opened {
            write_sse_headers(stream).await.map_err(|_| ())?;
            opened = true;
        }
        // Full, owner-checked snapshots make reconnect lossless without treating hints as facts.
        let frame = format!("event: terminal-account\ndata: {body}\n\n");
        write_sse(stream, frame.as_bytes(), state.config.request_timeout)
            .await
            .map_err(|_| ())?;
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = fallback.tick() => {},
            result = changes.changed() => if result.is_err() { return Err(()); },
        }
        // Coalesce a burst without starving a client while other accounts keep updating.
        tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            _ = time::sleep(Duration::from_millis(16)) => {},
        }
    }
}
