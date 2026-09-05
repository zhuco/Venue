use super::{ClientEvent, GridMutation, path};
use futures_util::StreamExt;
use serde::Deserialize;
use venue_control_protocol::support_martingale::{
    SUPPORT_MARTINGALE_SCHEMA_VERSION, SupportMartingaleInstance,
};

pub(crate) use venue_control_protocol::support_martingale::{
    SupportMartingaleConfig, SupportMartingaleCreateRequest, SupportMartingaleLifecycleRequest,
    SupportMartingaleListItem,
};
pub(crate) const INSTANCES_PATH: &str = "/v2/strategies/support-martingale/instances";
pub(crate) const LIFECYCLE_PATH: &str = "/v2/strategies/support-martingale/lifecycle";
const BODY_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
enum ListResponse {
    Items(Vec<SupportMartingaleListItem>),
    Instance(SupportMartingaleInstance),
}

pub(crate) async fn fetch(
    client: &reqwest::Client,
    endpoint: &str,
) -> Result<Vec<SupportMartingaleListItem>, Box<ClientEvent>> {
    let response = client
        .get(path(endpoint, INSTANCES_PATH))
        .send()
        .await
        .map_err(|_| unavailable(false, "支撑分批策略列表连接失败"))?;
    if response.status().as_u16() == 401 {
        return Err(Box::new(ClientEvent::SessionExpired));
    }
    if !response.status().is_success() {
        return Err(unavailable(false, "支撑分批策略接口暂不可用"));
    }
    match serde_json::from_slice::<ListResponse>(&bounded_body(response, false).await?)
        .map_err(|_| unavailable(false, "支撑分批策略响应无效"))?
    {
        ListResponse::Items(items) => Ok(items),
        ListResponse::Instance(instance) => Ok(vec![to_list_item(instance)]),
    }
}

pub(crate) async fn submit(
    client: &reqwest::Client,
    endpoint: &str,
    mutation: &GridMutation,
) -> Result<SupportMartingaleListItem, Box<ClientEvent>> {
    let (route, body) = match mutation {
        GridMutation::SupportMartingaleCreate(req) => (
            INSTANCES_PATH,
            serde_json::to_value(req).map_err(|_| unavailable(true, "请求编码失败"))?,
        ),
        GridMutation::SupportMartingaleLifecycle(req) => (
            LIFECYCLE_PATH,
            serde_json::to_value(req).map_err(|_| unavailable(true, "请求编码失败"))?,
        ),
        _ => return Err(unavailable(true, "请求类型不匹配")),
    };
    let response = client
        .post(path(endpoint, route))
        .json(&body)
        .send()
        .await
        .map_err(|_| unavailable(true, "支撑分批策略请求连接失败"))?;
    if response.status().as_u16() == 401 {
        return Err(Box::new(ClientEvent::SessionExpired));
    }
    if !response.status().is_success() {
        return Err(unavailable(true, "支撑分批策略请求未接受"));
    }
    match serde_json::from_slice::<ListResponse>(&bounded_body(response, true).await?)
        .map_err(|_| unavailable(true, "支撑分批策略响应无效"))?
    {
        ListResponse::Items(mut items) => items
            .pop()
            .ok_or_else(|| unavailable(true, "支撑分批策略响应为空")),
        ListResponse::Instance(instance) => Ok(to_list_item(instance)),
    }
}

pub(crate) fn schema_version() -> u16 {
    SUPPORT_MARTINGALE_SCHEMA_VERSION
}
fn to_list_item(instance: SupportMartingaleInstance) -> SupportMartingaleListItem {
    SupportMartingaleListItem {
        instance_id: instance.instance_id,
        execution_venue: instance.execution_venue,
        trading_account_id: instance.trading_account_id,
        lifecycle: instance.lifecycle,
        health: instance.health,
        revision: instance.revision,
        symbol_count: instance.symbols.len() as u32,
        reserved_budget: instance.reserved_budget,
    }
}
async fn bounded_body(
    response: reqwest::Response,
    mutation: bool,
) -> Result<Vec<u8>, Box<ClientEvent>> {
    if response
        .content_length()
        .is_some_and(|len| len > BODY_LIMIT as u64)
    {
        return Err(unavailable(mutation, "响应体过大"));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| unavailable(mutation, "响应体不可用"))?;
        if bytes.len().saturating_add(chunk.len()) > BODY_LIMIT {
            return Err(unavailable(mutation, "响应体过大"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
fn unavailable(mutation: bool, message: &str) -> Box<ClientEvent> {
    if mutation {
        Box::new(ClientEvent::SupportMartingaleMutationUnavailable(
            message.to_owned(),
        ))
    } else {
        Box::new(ClientEvent::SupportMartingaleUnavailable(
            message.to_owned(),
        ))
    }
}
