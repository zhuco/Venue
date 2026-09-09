use super::*;
use crate::client::{parse_sse_frame, sse_boundary};
use venue_control_protocol::kol::KOL_TERMINAL_ACCOUNT_STREAM_PATH;
use venue_control_protocol::terminal_account_stream::{COMPACT_QUERY, TerminalAccountStreamEvent};

const FRAME_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub(super) async fn receive(
    client: &reqwest::Client,
    endpoint: &str,
    request: &Scoped<TerminalProjectionRequest>,
    sender: &crossbeam_channel::Sender<ClientEvent>,
    context: &eframe::egui::Context,
) -> Result<(), TerminalReadError> {
    let response = tokio::time::timeout(
        super::super::REQUEST_TIMEOUT,
        client
            .post(format!(
                "{}?{COMPACT_QUERY}",
                path(endpoint, KOL_TERMINAL_ACCOUNT_STREAM_PATH)
            ))
            .json(&request.value)
            .send(),
    )
    .await
    .map_err(|_| terminal_unavailable("Account stream connect timed out"))?
    .map_err(|_| terminal_unavailable("Account stream unavailable"))?;
    if response.status().as_u16() == 401 {
        return Err(TerminalReadError::SessionExpired);
    }
    if !response.status().is_success() {
        return Err(terminal_unavailable("Account stream rejected"));
    }
    if !response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes().starts_with(b"text/event-stream"))
    {
        return Err(terminal_unavailable("Invalid account stream content type"));
    }
    let mut bytes = response.bytes_stream();
    let mut buffer = Vec::new();
    let mut projection = None;
    let mut deadline = tokio::time::Instant::now() + FRAME_TIMEOUT;
    let mut next_log = tokio::time::Instant::now();
    loop {
        // Partial bytes and SSE comments are not proof that an account frame arrived.
        let chunk = tokio::time::timeout_at(deadline, bytes.next())
            .await
            .map_err(|_| terminal_unavailable("Account stream heartbeat timed out"))?
            .ok_or_else(|| terminal_unavailable("Account stream closed"))?
            .map_err(|_| terminal_unavailable("Account stream read failed"))?;
        if buffer.len().saturating_add(chunk.len()) > BODY_LIMIT + 65536 {
            return Err(terminal_unavailable("Account stream too large"));
        }
        buffer.extend_from_slice(&chunk);
        while let Some((boundary, delimiter)) = sse_boundary(&buffer) {
            if boundary > BODY_LIMIT {
                return Err(terminal_unavailable("Account stream frame too large"));
            }
            let text = std::str::from_utf8(&buffer[..boundary])
                .map_err(|_| terminal_unavailable("Invalid account stream encoding"))?;
            let frame = parse_sse_frame(text)
                .map_err(|_| terminal_unavailable("Invalid account stream frame"))?;
            if let Some(payload) = frame.payload {
                let previous_observed = projection
                    .as_ref()
                    .map(|p: &TerminalAccountProjection| p.observed_ms);
                let update: TerminalAccountStreamEvent = serde_json::from_str(&payload)
                    .map_err(|_| terminal_unavailable("Invalid account stream projection"))?;
                update
                    .apply(&mut projection)
                    .map_err(|_| terminal_unavailable("Account stream baseline mismatch"))?;
                let event = ClientEvent::TerminalAccountProjection {
                    credential_id: request.value.credential_id.clone(),
                    projection: projection.clone(),
                };
                if !request.scope.accepts(&event)
                    || matches!(&event,
                    ClientEvent::TerminalAccountProjection { projection: Some(p), .. } if p.validate().is_err())
                {
                    return Err(terminal_unavailable("Account stream scope mismatch"));
                }
                if let ClientEvent::TerminalAccountProjection {
                    projection: Some(p),
                    ..
                } = &event
                {
                    crate::latency_evidence::projection_received(&request.scope, p);
                    if tokio::time::Instant::now() >= next_log {
                        tracing::info!(
                            target: "venueflow::account_projection",
                            frame_bytes = payload.len(),
                            source_observed_ms = p.observed_ms,
                            received_ms = crate::account_center::now_ms(),
                            "Account projection stream progress"
                        );
                        next_log = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
                    }
                }
                publish(sender, context, request.scope.event(event));
                if projection.is_none()
                    || projection.as_ref().map(|p| p.observed_ms) > previous_observed
                {
                    deadline = tokio::time::Instant::now() + FRAME_TIMEOUT;
                }
            }
            buffer.drain(..boundary + delimiter);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_scope::tests::{id, model, projection};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn partial_bytes_and_comments_cannot_keep_an_empty_account_stream_alive()
    -> Result<(), Box<dyn std::error::Error>> {
        let model = model();
        let request = Scoped {
            scope: model.confirmed_account_scope().ok_or("missing scope")?,
            value: TerminalProjectionRequest {
                schema_version: 1,
                credential_id: id(1),
                symbols: vec!["BTC/USDC".parse()?],
            },
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut input = [0; 4096];
            let _ = socket.read(&mut input).await?;
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n: heartbeat\n\ndata: ").await?;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                if socket.write_all(b" ").await.is_err() {
                    return Ok::<(), std::io::Error>(());
                }
            }
        });
        let (sender, events) = crossbeam_channel::unbounded();
        let client = reqwest::Client::builder().no_proxy().build()?;
        let result = tokio::time::timeout(
            FRAME_TIMEOUT + std::time::Duration::from_secs(2),
            receive(
                &client,
                &endpoint,
                &request,
                &sender,
                &eframe::egui::Context::default(),
            ),
        )
        .await;
        server.abort();
        assert!(
            matches!(result?, Err(TerminalReadError::Unavailable(message)) if message.contains("timed out"))
        );
        assert!(events.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn fragmented_snapshots_are_scoped_and_crossed_accounts_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let model = model();
        let request = Scoped {
            scope: model.confirmed_account_scope().ok_or("missing scope")?,
            value: TerminalProjectionRequest {
                schema_version: 1,
                credential_id: id(1),
                symbols: vec!["BTC/USDC".parse()?],
            },
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let original = projection(1);
        let expected = serde_json::to_string(&TerminalAccountStreamEvent::Snapshot(Some(
            original.clone(),
        )))?;
        let mut latest = original.clone();
        latest.observed_ms += 1;
        latest.persisted_ms += 1;
        let update = serde_json::to_string(&TerminalAccountStreamEvent::between(
            Some(&original),
            Some(&latest),
        ))?;
        let wrong =
            serde_json::to_string(&TerminalAccountStreamEvent::Snapshot(Some(projection(2))))?;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut input = [0; 4096];
            let _ = socket.read(&mut input).await?;
            assert!(
                std::str::from_utf8(&input).is_ok_and(|request| request.contains("?compact=1"))
            );
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await?;
            for frame in [
                format!("event: terminal-account\ndata: {expected}\n\n"),
                format!("event: terminal-account\ndata: {update}\n\n"),
                format!("event: terminal-account\ndata: {wrong}\n\n"),
            ] {
                for part in frame.as_bytes().chunks(13) {
                    socket.write_all(part).await?;
                    tokio::task::yield_now().await;
                }
            }
            Ok::<(), std::io::Error>(())
        });
        let (sender, events) = crossbeam_channel::unbounded();
        let client = reqwest::Client::builder().no_proxy().build()?;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            receive(
                &client,
                &endpoint,
                &request,
                &sender,
                &eframe::egui::Context::default(),
            ),
        )
        .await?;
        assert!(matches!(result, Err(TerminalReadError::Unavailable(_))));
        let published: Vec<_> = events.try_iter().collect();
        assert_eq!(published.len(), 2);
        assert!(
            matches!(&published[0], ClientEvent::AccountScoped { scope, .. } if scope == &request.scope)
        );
        server.await??;
        Ok(())
    }
}
