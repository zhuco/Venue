use super::{ClientEvent, path, publish};
use futures_util::StreamExt;
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

// A slow read endpoint must not stall command delivery. Only one read is in flight.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn start_native(
    client: reqwest::Client,
    endpoint: String,
    sender: crossbeam_channel::Sender<ClientEvent>,
    context: eframe::egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) {
    tokio::spawn(async move {
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            let event =
                match tokio::time::timeout(super::REQUEST_TIMEOUT, fetch(&client, &endpoint)).await
                {
                    Ok(Ok(facts)) => ClientEvent::ExecutionFacts(facts),
                    Ok(Err(event)) => *event,
                    Err(_) => *unavailable("Execution facts request timed out"),
                };
            if stop.load(std::sync::atomic::Ordering::Acquire) {
                break;
            }
            let expired = matches!(event, ClientEvent::SessionExpired);
            publish(&sender, &context, event);
            if expired {
                break;
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
