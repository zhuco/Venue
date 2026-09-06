use super::{ClientEvent, GridMutation, path};
use futures_util::StreamExt;
use serde::Deserialize;
use venue_control_protocol::accounts::{AccountErrorCode, AccountErrorResponse};
use venue_control_protocol::support_martingale::{
    SUPPORT_MARTINGALE_PREFLIGHT_PATH, SUPPORT_MARTINGALE_SCHEMA_VERSION,
    SupportMartingaleInstance, SupportMartingalePreflightResponse,
};

pub(crate) use venue_control_protocol::support_martingale::{
    SupportMartingaleConfig, SupportMartingaleCreateRequest, SupportMartingaleLifecycleRequest,
    SupportMartingaleListItem, SupportMartingalePreflightRequest,
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

pub(crate) enum SupportMartingaleSubmission {
    Instance(SupportMartingaleListItem),
    Preflight(SupportMartingalePreflightResponse),
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
    let status = response.status();
    if !status.is_success() {
        let body = bounded_body(response, false).await?;
        return Err(unavailable(false, &response_error(status.as_u16(), &body)));
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
) -> Result<SupportMartingaleSubmission, Box<ClientEvent>> {
    let (route, body) = match mutation {
        GridMutation::SupportMartingaleCreate(req) => (
            INSTANCES_PATH,
            serde_json::to_value(req).map_err(|_| unavailable(true, "请求编码失败"))?,
        ),
        GridMutation::SupportMartingaleLifecycle(req) => (
            LIFECYCLE_PATH,
            serde_json::to_value(req).map_err(|_| unavailable(true, "请求编码失败"))?,
        ),
        GridMutation::SupportMartingalePreflight(req) => (
            SUPPORT_MARTINGALE_PREFLIGHT_PATH,
            serde_json::to_value(req).map_err(|_| unavailable(true, "预检请求编码失败"))?,
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
    let status = response.status();
    if !status.is_success() {
        let body = bounded_body(response, true).await?;
        return Err(unavailable(true, &response_error(status.as_u16(), &body)));
    }
    let body = bounded_body(response, true).await?;
    if matches!(mutation, GridMutation::SupportMartingalePreflight(_)) {
        let response = serde_json::from_slice::<SupportMartingalePreflightResponse>(&body)
            .map_err(|_| unavailable(true, "预检响应无效"))?;
        response
            .validate()
            .map_err(|_| unavailable(true, "预检响应校验失败"))?;
        return Ok(SupportMartingaleSubmission::Preflight(response));
    }
    match serde_json::from_slice::<ListResponse>(&body)
        .map_err(|_| unavailable(true, "支撑分批策略响应无效"))?
    {
        ListResponse::Items(mut items) => items
            .pop()
            .map(SupportMartingaleSubmission::Instance)
            .ok_or_else(|| unavailable(true, "支撑分批策略响应为空")),
        ListResponse::Instance(instance) => Ok(SupportMartingaleSubmission::Instance(
            to_list_item(instance),
        )),
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
        health_reason: instance
            .symbols
            .iter()
            .find_map(|state| state.health_reason.clone()),
        revision: instance.revision,
        symbol_count: instance.symbols.len() as u32,
        reserved_budget: instance.reserved_budget,
        config: instance.config,
    }
}

fn response_error(status: u16, body: &[u8]) -> String {
    let code = serde_json::from_slice::<AccountErrorResponse>(body)
        .ok()
        .map(|response| response.code);
    let detail = match code {
        Some(AccountErrorCode::InvalidInput) => "请求字段或配置无效",
        Some(AccountErrorCode::InvalidLogin | AccountErrorCode::Unauthorized) => "登录已失效",
        Some(AccountErrorCode::Forbidden) => "当前用户无权操作此实例",
        Some(AccountErrorCode::NotFound) => "实例不存在或不属于当前用户",
        Some(AccountErrorCode::Conflict) => "实例状态、revision 或账户事实已变化",
        Some(AccountErrorCode::VerificationRequired) => "凭证或启动前签名事实未通过",
        Some(AccountErrorCode::AccountInUse) => "真实账户正被其他策略或未决命令占用",
        Some(AccountErrorCode::RateLimited) => "请求过于频繁",
        Some(AccountErrorCode::UsernameUnavailable | AccountErrorCode::Unavailable) | None => {
            "Control 或签名事实暂不可用"
        }
    };
    format!("{detail}（HTTP {status}）")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_failures_are_sanitized_without_echoing_server_content() {
        let body = serde_json::to_vec(&AccountErrorResponse {
            code: AccountErrorCode::AccountInUse,
        })
        .unwrap();
        let message = response_error(409, &body);
        assert!(message.contains("其他策略或未决命令占用"));
        assert!(!message.contains("secret"));
        assert!(
            response_error(503, br#"{"secret":"raw exchange error"}"#).contains("签名事实暂不可用")
        );
    }
}
