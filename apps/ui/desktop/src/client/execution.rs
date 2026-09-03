use super::{ClientEvent, path, publish};
use futures_util::StreamExt;
use venue_control_protocol::kol::{
    ExecutorCommandSummary, KOL_EXECUTION_STATUS_PATH, KOL_TERMINAL_ACCOUNT_PATH,
    TerminalAccountProjection, TerminalProjectionRequest,
};
use venue_control_protocol::{EXECUTION_FACTS_PATH, ExecutionFactsSnapshot};

const BODY_LIMIT: usize = 4 * 1024 * 1024;

async fn fetch(
    client: &reqwest::Client,
    endpoint: &str,
) -> Result<ExecutionFactsSnapshot, Box<ClientEvent>> {
    let response = client
        .get(path(endpoint, EXECUTION_FACTS_PATH))
        .send()
        .await
        .map_err(|_| unavailable("Execution facts connection failed"))?;
    if response.status().as_u16() == 401 {
        return Err(Box::new(ClientEvent::SessionExpired));
    }
    if !response.status().is_success() {
        return Err(unavailable(&format!(
            "Execution facts HTTP {}",
            response.status().as_u16()
        )));
    }
    if response
        .content_length()
        .is_some_and(|length| length > BODY_LIMIT as u64)
    {
        return Err(unavailable("Execution facts body too large"));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| unavailable("Execution facts body unavailable"))?;
        if bytes.len().saturating_add(chunk.len()) > BODY_LIMIT {
            return Err(unavailable("Execution facts body too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    decode(&bytes).map_err(|_| unavailable("Execution facts validation failed"))
}

fn decode(bytes: &[u8]) -> Result<ExecutionFactsSnapshot, ()> {
    if bytes.len() > BODY_LIMIT {
        return Err(());
    }
    let facts: ExecutionFactsSnapshot = serde_json::from_slice(bytes).map_err(|_| ())?;
    facts.validate().map_err(|_| ())?;
    Ok(facts)
}

fn unavailable(message: &str) -> Box<ClientEvent> {
    Box::new(ClientEvent::ExecutionFactsUnavailable(message.to_owned()))
}

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
    if projection
        .as_ref()
        .is_some_and(|value| value.validate().is_err())
    {
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

// Public facts, signed account state, and execution history have independent freshness needs.
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
    let facts_client = client.clone();
    let facts_endpoint = endpoint.clone();
    let facts_sender = sender.clone();
    let facts_context = context.clone();
    let facts_stop = stop.clone();
    tokio::spawn(async move {
        while !facts_stop.load(std::sync::atomic::Ordering::Acquire) {
            let event = match tokio::time::timeout(
                super::REQUEST_TIMEOUT,
                fetch(&facts_client, &facts_endpoint),
            )
            .await
            {
                Ok(Ok(facts)) => ClientEvent::ExecutionFacts(facts),
                Ok(Err(event)) => *event,
                Err(_) => *unavailable("Execution facts request timed out"),
            };
            if facts_stop.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            let expired = matches!(event, ClientEvent::SessionExpired);
            publish(&facts_sender, &facts_context, event);
            if expired {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

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
                Ok(Ok(executions)) => Some(ClientEvent::TerminalExecutions(executions)),
                Ok(Err(event)) if matches!(event.as_ref(), ClientEvent::SessionExpired) => {
                    Some(*event)
                }
                Ok(Err(_)) | Err(_) => None,
            };
            if history_stop.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            if let Some(event) = event {
                let expired = matches!(event, ClientEvent::SessionExpired);
                publish(&history_sender, &history_context, event);
                if expired {
                    break;
                }
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
                    Ok(Ok(projection)) => ClientEvent::TerminalAccountProjection(projection),
                    Ok(Err(event)) => *event,
                    Err(_) => *terminal_unavailable("Private account projection request timed out"),
                };
                let expired = matches!(event, ClientEvent::SessionExpired);
                publish(&sender, &context, event);
                if expired {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });
}

#[cfg(target_arch = "wasm32")]
pub(super) fn start_web(
    endpoint: String,
    sender: crossbeam_channel::Sender<ClientEvent>,
    context: eframe::egui::Context,
    stop: std::rc::Rc<std::cell::Cell<bool>>,
    token: venue_control_protocol::accounts::SecretValue,
) {
    wasm_bindgen_futures::spawn_local(async move {
        let Ok(headers) = crate::account_client::authorization_headers(Some(&token)) else {
            return;
        };
        let Ok(client) = reqwest::Client::builder().default_headers(headers).build() else {
            return;
        };
        while !stop.get() {
            let event = match futures_util::future::select(
                Box::pin(fetch(&client, &endpoint)),
                Box::pin(super::wasm_timer(10_000)),
            )
            .await
            {
                futures_util::future::Either::Left((Ok(facts), _)) => {
                    ClientEvent::ExecutionFacts(facts)
                }
                futures_util::future::Either::Left((Err(event), _)) => *event,
                futures_util::future::Either::Right(_) => {
                    *unavailable("Execution facts request timed out")
                }
            };
            if stop.get() {
                break;
            }
            let expired = matches!(event, ClientEvent::SessionExpired);
            publish(&sender, &context, event);
            if expired {
                break;
            }
            super::wasm_timer(1_000).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn facts_use_canonical_validation_and_bounded_body() {
        let fixture = br#"{"schema_version":2,"generated_ms":1,"orders":[],"positions":[],"fills":[],"reconciliation":[],"copy_ledger":[],"drift":[],"execution":[],"risk":[],"health":[]}"#;
        assert!(decode(fixture).is_ok());
        assert!(decode(b"{}").is_err());
        assert!(decode(&vec![0; BODY_LIMIT + 1]).is_err());
    }
}
