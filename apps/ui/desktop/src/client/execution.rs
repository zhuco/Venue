use super::{ClientEvent, path, publish};
use futures_util::StreamExt;
use venue_control_protocol::kol::{
    ExecutorCommandSummary, KOL_EXECUTION_STATUS_PATH, KOL_TERMINAL_ACCOUNT_PATH,
    TerminalAccountProjection, TerminalProjectionRequest,
};

const BODY_LIMIT: usize = 4 * 1024 * 1024;

fn terminal_unavailable(message: &str) -> Box<ClientEvent> {
    Box::new(ClientEvent::TerminalAccountUnavailable(message.to_owned()))
}

async fn fetch_terminal_projection(
    client: &reqwest::Client,
    endpoint: &str,
    request: &TerminalProjectionRequest,
) -> Result<Option<TerminalAccountProjection>, Box<ClientEvent>> {
    let response = client
        .post(path(endpoint, KOL_TERMINAL_ACCOUNT_PATH))
        .json(request)
        .send()
        .await
        .map_err(|_| terminal_unavailable("Private account projection connection failed"))?;
    if response.status().as_u16() == 401 {
        return Err(Box::new(ClientEvent::SessionExpired));
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
) -> Result<Vec<ExecutorCommandSummary>, Box<ClientEvent>> {
    let response = client
        .get(path(endpoint, KOL_EXECUTION_STATUS_PATH))
        .send()
        .await
        .map_err(|_| terminal_unavailable("Terminal execution history connection failed"))?;
    if response.status().as_u16() == 401 {
        return Err(Box::new(ClientEvent::SessionExpired));
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

async fn bounded_body(response: reqwest::Response) -> Result<Vec<u8>, Box<ClientEvent>> {
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
    projection_requests: crossbeam_channel::Receiver<TerminalProjectionRequest>,
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
                Ok(Err(event)) if matches!(event.as_ref(), ClientEvent::SessionExpired) => *event,
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

    tokio::spawn(async move {
        let mut projection_request = None;
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            for request in projection_requests.try_iter() {
                projection_request = Some(request);
            }
            if let Some(request) = projection_request.as_ref() {
                let event = match tokio::time::timeout(
                    super::REQUEST_TIMEOUT,
                    fetch_terminal_projection(&client, &endpoint, request),
                )
                .await
                {
                    Ok(Ok(projection)) => ClientEvent::TerminalAccountProjection {
                        credential_id: request.credential_id.clone(),
                        projection,
                    },
                    Ok(Err(event)) => *event,
                    Err(_) => *terminal_unavailable("Private account projection request timed out"),
                };
                if stop.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                let expired = matches!(event, ClientEvent::SessionExpired);
                let latest = projection_requests.try_iter().last();
                if let Some(latest) = latest {
                    let changed = latest != *request;
                    projection_request = Some(latest);
                    if changed && !expired {
                        continue;
                    }
                }
                publish(&sender, &context, event);
                if expired {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });
}
