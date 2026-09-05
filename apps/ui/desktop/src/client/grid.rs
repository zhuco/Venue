use super::{ClientEvent, path, publish};
use futures_util::StreamExt;
use venue_control_protocol::accounts::{AccountErrorCode, AccountErrorResponse};
use venue_control_protocol::grid::{
    GRID_INSTANCES_PATH, GRID_LIFECYCLE_PATH, GridConfigUpdateRequest, GridInstanceCreateRequest,
    GridInstanceSummary, GridLifecycleRequest,
};

const BODY_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) enum GridMutation {
    Create(GridInstanceCreateRequest),
    Update(GridConfigUpdateRequest),
    Lifecycle(GridLifecycleRequest),
    LeaderCreate(venue_control_protocol::leader_bot::LeaderBotConfiguredCreateRequest),
    LeaderUpdate(venue_control_protocol::leader_bot::LeaderBotUpdateRequest),
    LeaderLifecycle(venue_control_protocol::leader_bot::LeaderBotLifecycleRequest),
}

impl GridMutation {
    pub(crate) fn validate(&self) -> bool {
        match self {
            Self::Create(request) => request.validate().is_ok(),
            Self::Update(request) => request.validate().is_ok(),
            Self::Lifecycle(request) => request.validate().is_ok(),
            Self::LeaderCreate(request) => request.valid(),
            Self::LeaderUpdate(request) => request.valid(),
            Self::LeaderLifecycle(request) => request.valid(),
        }
    }

    fn endpoint(&self) -> &'static str {
        match self {
            Self::Create(_) | Self::Update(_) => GRID_INSTANCES_PATH,
            Self::Lifecycle(_) => GRID_LIFECYCLE_PATH,
            Self::LeaderCreate(_) => venue_control_protocol::leader_bot::LEADER_BOTS_PATH,
            Self::LeaderUpdate(_) => venue_control_protocol::leader_bot::LEADER_BOTS_UPDATE_PATH,
            Self::LeaderLifecycle(_) => {
                venue_control_protocol::leader_bot::LEADER_BOTS_LIFECYCLE_PATH
            }
        }
    }

    fn matches(&self, summary: &GridInstanceSummary) -> bool {
        match self {
            Self::Create(request) => {
                summary.credential_id == request.credential_id && summary.symbol == request.symbol
            }
            Self::Update(request) => summary.instance_id == request.instance_id,
            Self::Lifecycle(request) => summary.instance_id == request.instance_id,
            Self::LeaderCreate(_) | Self::LeaderUpdate(_) | Self::LeaderLifecycle(_) => false,
        }
    }
}

async fn fetch_instances(
    client: &reqwest::Client,
    endpoint: &str,
) -> Result<Vec<GridInstanceSummary>, Box<ClientEvent>> {
    let response = client
        .get(path(endpoint, GRID_INSTANCES_PATH))
        .send()
        .await
        .map_err(|_| list_unavailable("Grid instances connection failed"))?;
    if response.status().as_u16() == 401 {
        return Err(Box::new(ClientEvent::SessionExpired));
    }
    let status = response.status();
    if !status.is_success() {
        let body = bounded_body(response, false).await?;
        return Err(list_unavailable(&response_error(status.as_u16(), &body)));
    }
    let instances =
        serde_json::from_slice::<Vec<GridInstanceSummary>>(&bounded_body(response, false).await?)
            .map_err(|_| list_unavailable("Grid instances response is invalid"))?;
    if instances
        .iter()
        .any(|instance| instance.validate().is_err())
    {
        return Err(list_unavailable("Grid instances validation failed"));
    }
    Ok(instances)
}

async fn submit(
    client: &reqwest::Client,
    endpoint: &str,
    mutation: &GridMutation,
) -> Result<GridInstanceSummary, Box<ClientEvent>> {
    let builder = client.post(path(endpoint, mutation.endpoint()));
    let response = match mutation {
        GridMutation::Create(request) => builder.json(request),
        GridMutation::Update(request) => builder.json(request),
        GridMutation::Lifecycle(request) => builder.json(request),
        GridMutation::LeaderCreate(_)
        | GridMutation::LeaderUpdate(_)
        | GridMutation::LeaderLifecycle(_) => {
            return Err(mutation_unavailable("wrong mutation route"));
        }
    }
    .send()
    .await
    .map_err(|_| mutation_unavailable("Grid request connection failed"))?;
    if response.status().as_u16() == 401 {
        return Err(Box::new(ClientEvent::SessionExpired));
    }
    let status = response.status();
    if !status.is_success() {
        let body = bounded_body(response, true).await?;
        return Err(mutation_unavailable(&response_error(
            status.as_u16(),
            &body,
        )));
    }
    let summary =
        serde_json::from_slice::<GridInstanceSummary>(&bounded_body(response, true).await?)
            .map_err(|_| mutation_unavailable("Grid response is invalid"))?;
    if summary.validate().is_err() || !mutation.matches(&summary) {
        return Err(mutation_unavailable("Grid response validation failed"));
    }
    Ok(summary)
}

async fn bounded_body(
    response: reqwest::Response,
    mutation: bool,
) -> Result<Vec<u8>, Box<ClientEvent>> {
    if response
        .content_length()
        .is_some_and(|length| length > BODY_LIMIT as u64)
    {
        return Err(body_unavailable(
            mutation,
            "Grid response body is too large",
        ));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|_| body_unavailable(mutation, "Grid response body is unavailable"))?;
        if bytes.len().saturating_add(chunk.len()) > BODY_LIMIT {
            return Err(body_unavailable(
                mutation,
                "Grid response body is too large",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn body_unavailable(mutation: bool, message: &str) -> Box<ClientEvent> {
    if mutation {
        mutation_unavailable(message)
    } else {
        list_unavailable(message)
    }
}

fn response_error(status: u16, body: &[u8]) -> String {
    let code = serde_json::from_slice::<AccountErrorResponse>(body)
        .ok()
        .map(|response| response.code);
    let detail = match code {
        Some(AccountErrorCode::InvalidInput) => "invalid_request",
        Some(AccountErrorCode::InvalidLogin | AccountErrorCode::Unauthorized) => "unauthorized",
        Some(AccountErrorCode::Forbidden) => "forbidden_scope",
        Some(AccountErrorCode::NotFound) => "instance_not_found",
        Some(AccountErrorCode::Conflict) => "state_or_revision_conflict",
        Some(AccountErrorCode::VerificationRequired) => "credential_verification_required",
        Some(AccountErrorCode::AccountInUse) => "account_owned_by_legacy_runtime",
        Some(AccountErrorCode::RateLimited) => "rate_limited",
        Some(AccountErrorCode::UsernameUnavailable | AccountErrorCode::Unavailable) | None => {
            "service_unavailable"
        }
    };
    format!("Grid request rejected [{detail}; HTTP {status}]")
}

fn list_unavailable(message: &str) -> Box<ClientEvent> {
    Box::new(ClientEvent::GridUnavailable(message.to_owned()))
}

fn mutation_unavailable(message: &str) -> Box<ClientEvent> {
    Box::new(ClientEvent::GridMutationUnavailable(message.to_owned()))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn start_native(
    client: reqwest::Client,
    endpoint: String,
    sender: crossbeam_channel::Sender<ClientEvent>,
    context: eframe::egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    mutations: crossbeam_channel::Receiver<GridMutation>,
) {
    let poll_client = client.clone();
    let poll_endpoint = endpoint.clone();
    let poll_sender = sender.clone();
    let poll_context = context.clone();
    let poll_stop = stop.clone();
    tokio::spawn(async move {
        while !poll_stop.load(std::sync::atomic::Ordering::Acquire) {
            let event = match tokio::time::timeout(
                super::REQUEST_TIMEOUT,
                fetch_instances(&poll_client, &poll_endpoint),
            )
            .await
            {
                Ok(Ok(instances)) => ClientEvent::GridInstances(instances),
                Ok(Err(event)) => *event,
                Err(_) => *list_unavailable("Grid instances request timed out"),
            };
            let expired = matches!(event, ClientEvent::SessionExpired);
            publish(&poll_sender, &poll_context, event);
            if expired {
                break;
            }
            let event = match tokio::time::timeout(
                super::REQUEST_TIMEOUT,
                super::leader_bot::fetch(&poll_client, &poll_endpoint),
            )
            .await
            {
                Ok(event) => event,
                Err(_) => ClientEvent::LeaderBotUnavailable {
                    mutation: false,
                    definitive: false,
                    message: "带单权限查询超时".into(),
                },
            };
            publish(&poll_sender, &poll_context, event);
            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        }
    });

    tokio::spawn(async move {
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            for mutation in mutations.try_iter().take(16) {
                if matches!(
                    mutation,
                    GridMutation::LeaderCreate(_)
                        | GridMutation::LeaderUpdate(_)
                        | GridMutation::LeaderLifecycle(_)
                ) {
                    let event = match tokio::time::timeout(
                        super::REQUEST_TIMEOUT,
                        super::leader_bot::submit(&client, &endpoint, &mutation),
                    )
                    .await
                    {
                        Ok(event) => event,
                        Err(_) => ClientEvent::LeaderBotUnavailable {
                            mutation: true,
                            definitive: false,
                            message: "带单操作未确认，可重试原请求".into(),
                        },
                    };
                    publish(&sender, &context, event);
                    continue;
                }
                let event = match tokio::time::timeout(
                    super::REQUEST_TIMEOUT,
                    submit(&client, &endpoint, &mutation),
                )
                .await
                {
                    Ok(Ok(summary)) => ClientEvent::GridMutationApplied(Box::new(summary)),
                    Ok(Err(event)) => *event,
                    Err(_) => *mutation_unavailable("Grid request timed out"),
                };
                let expired = matches!(event, ClientEvent::SessionExpired);
                publish(&sender, &context, event);
                if expired {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
    mutations: crossbeam_channel::Receiver<GridMutation>,
) {
    wasm_bindgen_futures::spawn_local(async move {
        let Ok(headers) = crate::account_client::authorization_headers(Some(&token)) else {
            return;
        };
        let Ok(client) = reqwest::Client::builder().default_headers(headers).build() else {
            return;
        };
        let mut next_poll_ms = 0_u64;
        while !stop.get() {
            let now_ms = crate::account_center::now_ms();
            if now_ms >= next_poll_ms {
                let event = match fetch_instances(&client, &endpoint).await {
                    Ok(instances) => ClientEvent::GridInstances(instances),
                    Err(event) => *event,
                };
                let expired = matches!(event, ClientEvent::SessionExpired);
                publish(&sender, &context, event);
                if expired {
                    break;
                }
                next_poll_ms = now_ms.saturating_add(3_000);
                let event = super::leader_bot::fetch(&client, &endpoint).await;
                publish(&sender, &context, event);
            }
            for mutation in mutations.try_iter().take(16) {
                if matches!(
                    mutation,
                    GridMutation::LeaderCreate(_)
                        | GridMutation::LeaderUpdate(_)
                        | GridMutation::LeaderLifecycle(_)
                ) {
                    let event = super::leader_bot::submit(&client, &endpoint, &mutation).await;
                    publish(&sender, &context, event);
                    continue;
                }
                let event = match submit(&client, &endpoint, &mutation).await {
                    Ok(summary) => ClientEvent::GridMutationApplied(Box::new(summary)),
                    Err(event) => *event,
                };
                let expired = matches!(event, ClientEvent::SessionExpired);
                publish(&sender, &context, event);
                if expired {
                    return;
                }
            }
            super::wasm_timer(100).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_error_is_diagnostic_without_echoing_response_body() {
        let body = br#"{"code":"conflict","secret":"must-not-be-rendered"}"#;
        let message = response_error(409, body);
        assert!(message.contains("state_or_revision_conflict"));
        assert!(message.contains("HTTP 409"));
        assert!(!message.contains("must-not-be-rendered"));
    }

    #[test]
    fn malformed_error_body_is_reduced_to_safe_status() {
        let message = response_error(503, b"upstream key=must-not-be-rendered");
        assert_eq!(
            message,
            "Grid request rejected [service_unavailable; HTTP 503]"
        );
    }
}
