use super::{ClientEvent, path};
use futures_util::StreamExt;
use venue_control_protocol::inventory_mm::*;

#[derive(Clone, Debug)]
pub(crate) enum Mutation {
    Create(InventoryMmCreateRequest),
    Preflight(InventoryMmPreflightRequest),
    Lifecycle(InventoryMmLifecycleRequest),
}
impl Mutation {
    pub(crate) fn valid(&self) -> bool {
        match self {
            Self::Create(request) => request.validate().is_ok(),
            Self::Lifecycle(request) => request.validate().is_ok(),
            Self::Preflight(request) => {
                request.expected_revision > 0
                    && venue_domain::is_canonical_trading_account_id(&request.instance_id)
            }
        }
    }
    pub(crate) fn route(&self) -> &'static str {
        match self {
            Self::Create(_) => INVENTORY_MM_PATH,
            Self::Preflight(_) => INVENTORY_MM_PREFLIGHT_PATH,
            Self::Lifecycle(_) => INVENTORY_MM_LIFECYCLE_PATH,
        }
    }
}
#[derive(Clone, Debug)]
pub enum Event {
    Instances(Vec<InventoryMmInstance>),
    Applied(Box<InventoryMmInstance>),
    Preflight(InventoryMmPreflight),
    Error {
        mutation: bool,
        uncertain: bool,
        message: String,
    },
}
fn error(mutation: bool, uncertain: bool, message: impl Into<String>) -> ClientEvent {
    ClientEvent::InventoryMm(Event::Error {
        mutation,
        uncertain,
        message: message.into(),
    })
}
pub(crate) fn timeout(mutation: Option<&Mutation>) -> ClientEvent {
    let uncertain = mutation.is_some_and(|value| !matches!(value, Mutation::Preflight(_)));
    error(
        mutation.is_some(),
        uncertain,
        if uncertain {
            "库存做市控制请求结果不确定；不会自动重发，请先核对实例状态"
        } else {
            "库存做市读取超时；未发送交易请求"
        },
    )
}
async fn body(response: reqwest::Response) -> Result<Vec<u8>, ()> {
    const LIMIT: usize = 4 * 1024 * 1024;
    if response
        .content_length()
        .is_some_and(|len| len > LIMIT as u64)
    {
        return Err(());
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ())?;
        if bytes.len().saturating_add(chunk.len()) > LIMIT {
            return Err(());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
pub(crate) async fn fetch(client: &reqwest::Client, endpoint: &str) -> ClientEvent {
    let Ok(response) = client.get(path(endpoint, INVENTORY_MM_PATH)).send().await else {
        return timeout(None);
    };
    if response.status().as_u16() == 401 {
        return ClientEvent::SessionExpired;
    }
    if !response.status().is_success() {
        return error(
            false,
            false,
            format!("库存做市列表不可用（HTTP {}）", response.status().as_u16()),
        );
    }
    match body(response)
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Vec<InventoryMmInstance>>(&bytes).ok())
    {
        Some(items) if items.iter().all(valid_instance) => {
            ClientEvent::InventoryMm(Event::Instances(items))
        }
        _ => error(false, false, "库存做市列表响应无效"),
    }
}
fn valid_instance(item: &InventoryMmInstance) -> bool {
    item.config.validate().is_ok()
        && item.revision > 0
        && venue_domain::is_canonical_trading_account_id(&item.instance_id)
        && venue_domain::is_canonical_trading_account_id(&item.credential_id)
        && venue_domain::is_canonical_trading_account_id(&item.trading_account_id)
}
pub(crate) async fn submit(
    client: &reqwest::Client,
    endpoint: &str,
    mutation: &Mutation,
) -> ClientEvent {
    let builder = client.post(path(endpoint, mutation.route()));
    let builder = match mutation {
        Mutation::Create(request) => builder.json(request),
        Mutation::Preflight(request) => builder.json(request),
        Mutation::Lifecycle(request) => builder.json(request),
    };
    let Ok(response) = builder.send().await else {
        return timeout(Some(mutation));
    };
    let status = response.status().as_u16();
    if status == 401 {
        return ClientEvent::SessionExpired;
    }
    if !(200..300).contains(&status) {
        return error(
            true,
            status >= 500 && !matches!(mutation, Mutation::Preflight(_)),
            format!("库存做市请求未通过（HTTP {status}）；请核对权限、实例版本和签名预检"),
        );
    }
    let Ok(bytes) = body(response).await else {
        return timeout(Some(mutation));
    };
    if let Mutation::Preflight(request) = mutation {
        return match serde_json::from_slice::<InventoryMmPreflight>(&bytes) {
            Ok(result)
                if result.instance_id == request.instance_id
                    && result.revision == request.expected_revision
                    && result.checked_ms > 0
                    && (!result.ready || result.blockers.is_empty()) =>
            {
                ClientEvent::InventoryMm(Event::Preflight(result))
            }
            _ => error(true, false, "库存做市预检响应无效"),
        };
    }
    match serde_json::from_slice::<InventoryMmInstance>(&bytes) {
        Ok(item)
            if valid_instance(&item)
                && match mutation {
                    Mutation::Create(request) => {
                        item.credential_id == request.credential_id && item.config == request.config
                    }
                    Mutation::Lifecycle(request) => {
                        item.instance_id == request.instance_id
                            && item.revision >= request.expected_revision
                    }
                    Mutation::Preflight(_) => false,
                } =>
        {
            ClientEvent::InventoryMm(Event::Applied(Box::new(item)))
        }
        _ => timeout(Some(mutation)),
    }
}
