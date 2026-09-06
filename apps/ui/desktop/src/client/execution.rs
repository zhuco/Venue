use super::{ClientEvent, path, publish};
use futures_util::StreamExt;
use venue_control_protocol::kol::{
    ExecutorCommandSummary, KOL_EXECUTION_STATUS_PATH, KOL_TERMINAL_ACCOUNT_PATH,
    TerminalAccountProjection, TerminalProjectionRequest,
};

const BODY_LIMIT: usize = 4 * 1024 * 1024;

enum TerminalReadError {
    SessionExpired,
    Unavailable(String),
}

fn terminal_unavailable(message: &str) -> TerminalReadError {
    #[cfg(not(target_arch = "wasm32"))]
    tracing::warn!(reason = message, "terminal account read unavailable");
    TerminalReadError::Unavailable(message.to_owned())
}

async fn fetch_terminal_projection(
    client: &reqwest::Client,
    endpoint: &str,
    request: &TerminalProjectionRequest,
) -> Result<Option<TerminalAccountProjection>, TerminalReadError> {
    let response = client
        .post(path(endpoint, KOL_TERMINAL_ACCOUNT_PATH))
        .json(request)
        .send()
        .await
        .map_err(|_| terminal_unavailable("Private account projection connection failed"))?;
    if response.status().as_u16() == 401 {
        return Err(TerminalReadError::SessionExpired);
    }
    if !response.status().is_success() {
        return Err(terminal_unavailable(&format!(
            "Private account projection HTTP {}",
            response.status().as_u16()
        )));
    }
    let bytes = bounded_body(response).await?;
    let projection: Option<TerminalAccountProjection> = serde_json::from_slice(&bytes)
        .map_err(|_| terminal_unavailable("Private account projection validation failed"))?;
    if projection.as_ref().is_some_and(|value| {
        value.validate().is_err() || value.credential_id != request.credential_id
    }) {
        return Err(terminal_unavailable(
            "Private account projection validation failed",
        ));
    }
    Ok(projection)
}

async fn fetch_terminal_executions(
    client: &reqwest::Client,
    endpoint: &str,
) -> Result<Vec<ExecutorCommandSummary>, TerminalReadError> {
    let response = client
        .get(path(endpoint, KOL_EXECUTION_STATUS_PATH))
        .send()
        .await
        .map_err(|_| terminal_unavailable("Terminal execution history connection failed"))?;
    if response.status().as_u16() == 401 {
        return Err(TerminalReadError::SessionExpired);
    }
    if !response.status().is_success() {
        return Err(terminal_unavailable(&format!(
            "Terminal execution history HTTP {}",
            response.status().as_u16()
        )));
    }
    let values: Vec<ExecutorCommandSummary> =
        serde_json::from_slice(&bounded_body(response).await?)
            .map_err(|_| terminal_unavailable("Terminal execution history validation failed"))?;
    if values.iter().any(|value| value.validate().is_err()) {
        return Err(terminal_unavailable(
            "Terminal execution history validation failed",
        ));
    }
    Ok(values)
}

async fn bounded_body(response: reqwest::Response) -> Result<Vec<u8>, TerminalReadError> {
    if response
        .content_length()
        .is_some_and(|length| length > BODY_LIMIT as u64)
    {
        return Err(terminal_unavailable("Private response body too large"));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| terminal_unavailable("Private response body unavailable"))?;
        if bytes.len().saturating_add(chunk.len()) > BODY_LIMIT {
            return Err(terminal_unavailable("Private response body too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

// Signed account state and execution history have independent freshness needs.
// Keeping their polls independent prevents one slow response from aging the others past the
// trading safety window; command delivery already runs on a separate task.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn start_native(
    client: reqwest::Client,
    endpoint: String,
    sender: crossbeam_channel::Sender<ClientEvent>,
    context: eframe::egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    projection_requests: tokio::sync::watch::Receiver<Option<TerminalProjectionRequest>>,
) {
    let history_client = client.clone();
    let history_endpoint = endpoint.clone();
    let history_sender = sender.clone();
    let history_context = context.clone();
    let history_stop = stop.clone();
    tokio::spawn(async move {
        while !history_stop.load(std::sync::atomic::Ordering::Acquire) {
            let event = match tokio::time::timeout(
                super::REQUEST_TIMEOUT,
                fetch_terminal_executions(&history_client, &history_endpoint),
            )
            .await
            {
                Ok(Ok(executions)) => ClientEvent::TerminalExecutions(executions),
                Ok(Err(TerminalReadError::SessionExpired)) => ClientEvent::SessionExpired,
                Ok(Err(_)) => ClientEvent::TerminalExecutionsUnavailable(
                    "历史委托读取失败，请检查 Control 连接。".into(),
                ),
                Err(_) => ClientEvent::TerminalExecutionsUnavailable(
                    "历史委托读取超时，显示的记录可能已过期。".into(),
                ),
            };
            if history_stop.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            let expired = matches!(event, ClientEvent::SessionExpired);
            publish(&history_sender, &history_context, event);
            if expired {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    });

    tokio::spawn(projection_loop(
        client,
        endpoint,
        sender,
        context,
        stop,
        projection_requests,
    ));
}

#[cfg(not(target_arch = "wasm32"))]
async fn projection_loop(
    client: reqwest::Client,
    endpoint: String,
    sender: crossbeam_channel::Sender<ClientEvent>,
    context: eframe::egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    mut requests: tokio::sync::watch::Receiver<Option<TerminalProjectionRequest>>,
) {
    while !stop.load(std::sync::atomic::Ordering::Acquire) {
        let request = requests.borrow_and_update().clone();
        if let Some(request) = request {
            // Only read-only projection requests are cancelled. Order delivery stays on
            // its independent durable command path. Watch retains the latest selection.
            let result = tokio::select! {
                biased;
                changed = requests.changed() => {
                    if changed.is_err() { break; }
                    continue;
                }
                result = tokio::time::timeout(
                    super::REQUEST_TIMEOUT,
                    fetch_terminal_projection(&client, &endpoint, &request),
                ) => result,
            };
            if stop.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            let event = match result {
                Ok(Ok(projection)) => ClientEvent::TerminalAccountProjection {
                    credential_id: request.credential_id,
                    projection,
                },
                Ok(Err(TerminalReadError::SessionExpired)) => ClientEvent::SessionExpired,
                Ok(Err(TerminalReadError::Unavailable(message))) => {
                    ClientEvent::TerminalAccountUnavailable {
                        credential_id: request.credential_id,
                        message,
                    }
                }
                Err(_) => ClientEvent::TerminalAccountUnavailable {
                    credential_id: request.credential_id,
                    message: "Private account projection request timed out".into(),
                },
            };
            let expired = matches!(event, ClientEvent::SessionExpired);
            if !expired && requests.has_changed().unwrap_or(true) {
                continue;
            }
            publish(&sender, &context, event);
            if expired {
                break;
            }
        }
        tokio::select! {
            changed = requests.changed() => {
                if changed.is_err() { break; }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::{
        sync::{Arc, atomic::AtomicBool},
        time::Duration,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn account_switch_interrupts_slow_read_and_poll_delay()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let mut old_connection = None;
            for index in 0..3 {
                let (mut socket, _) = listener.accept().await?;
                let mut bytes = Vec::new();
                loop {
                    let mut chunk = [0; 2048];
                    let read = socket.read(&mut chunk).await?;
                    if read == 0 {
                        return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                    }
                    bytes.extend_from_slice(&chunk[..read]);
                    if bytes.ends_with(b"}") {
                        break;
                    }
                }
                let _ = seen_tx.send(String::from_utf8_lossy(&bytes).to_string());
                if index == 0 {
                    old_connection = Some(socket);
                } else {
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\nnull").await?;
                }
            }
            drop(old_connection);
            Ok::<_, std::io::Error>(())
        });
        let symbol: venue_domain::Symbol = "DOGE/USDC".parse()?;
        let request = |index| TerminalProjectionRequest {
            schema_version: venue_control_protocol::kol::TERMINAL_PROJECTION_SCHEMA_VERSION,
            credential_id: format!("00000000-0000-4000-8000-{index:012}"),
            symbols: vec![symbol.clone()],
        };
        let (requests_tx, requests_rx) = tokio::sync::watch::channel(Some(request(1)));
        let (events_tx, events_rx) = crossbeam_channel::unbounded();
        let worker = tokio::spawn(projection_loop(
            reqwest::Client::builder().no_proxy().build()?,
            endpoint,
            events_tx,
            egui::Context::default(),
            Arc::new(AtomicBool::new(false)),
            requests_rx,
        ));
        let first = tokio::time::timeout(Duration::from_secs(2), seen_rx.recv())
            .await?
            .ok_or("first request")?;
        assert!(first.contains(&request(1).credential_id));
        for index in [2, 3] {
            requests_tx.send(Some(request(index)))?;
            let received = tokio::time::timeout(Duration::from_millis(750), seen_rx.recv())
                .await?
                .ok_or("switched request")?;
            assert!(received.contains(&request(index).credential_id));
            let event = tokio::time::timeout(Duration::from_millis(750), async {
                loop {
                    if let Ok(event) = events_rx.try_recv() {
                        break event;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
            assert!(
                matches!(event, ClientEvent::TerminalAccountProjection { credential_id, projection: None } if credential_id == request(index).credential_id)
            );
        }
        drop(requests_tx);
        tokio::time::timeout(Duration::from_secs(1), worker).await??;
        server.await??;
        assert!(events_rx.is_empty());
        Ok(())
    }
}
