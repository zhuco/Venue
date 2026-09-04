use super::{ClientEvent, grid::GridMutation, path};
use futures_util::StreamExt;
use venue_control_protocol::leader_bot::*;

pub(super) async fn fetch(client: &reqwest::Client, endpoint: &str) -> ClientEvent {
    response(
        client.get(path(endpoint, LEADER_BOT_PATH)).send().await,
        false,
    )
    .await
}
pub(super) async fn submit(
    client: &reqwest::Client,
    endpoint: &str,
    mutation: &GridMutation,
) -> ClientEvent {
    let result = match mutation {
        GridMutation::LeaderCreate(request) => {
            client
                .post(path(endpoint, LEADER_BOT_PATH))
                .json(request)
                .send()
                .await
        }
        GridMutation::LeaderLifecycle(request) => {
            client
                .post(path(endpoint, LEADER_BOT_LIFECYCLE_PATH))
                .json(request)
                .send()
                .await
        }
        _ => return unavailable(true, "无效的带单机器人请求"),
    };
    response(result, true).await
}
fn unavailable(mutation: bool, message: &str) -> ClientEvent {
    ClientEvent::LeaderBotUnavailable {
        mutation,
        definitive: false,
        message: message.into(),
    }
}
async fn response(
    result: Result<reqwest::Response, reqwest::Error>,
    mutation: bool,
) -> ClientEvent {
    let Ok(response) = result else {
        return unavailable(
            mutation,
            if mutation {
                "带单请求未确认；重试将复用原请求编号"
            } else {
                "带单权限查询连接失败，正在重试"
            },
        );
    };
    if response.status().as_u16() == 401 {
        return ClientEvent::SessionExpired;
    }
    if !response.status().is_success() {
        let status = response.status().as_u16();
        return ClientEvent::LeaderBotUnavailable {
            mutation,
            definitive: (400..500).contains(&status) && !matches!(status, 408 | 429),
            message: if mutation {
                format!("带单操作被拒绝（HTTP {status}）")
            } else if status == 404 {
                "当前服务器未提供带单机器人接口（HTTP 404）".into()
            } else {
                format!("带单权限查询失败（HTTP {status}），请检查服务版本与授权状态")
            },
        };
    }
    if response
        .content_length()
        .is_some_and(|length| length > 64 * 1024)
    {
        return unavailable(mutation, "带单响应过大");
    }
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let Ok(chunk) = chunk else {
            return unavailable(mutation, "带单响应中断");
        };
        if body.len().saturating_add(chunk.len()) > 64 * 1024 {
            return unavailable(mutation, "带单响应过大");
        }
        body.extend_from_slice(&chunk);
    }
    let Ok(access) = serde_json::from_slice::<LeaderBotAccess>(&body) else {
        return unavailable(mutation, "带单响应无效");
    };
    if access.schema_version != LEADER_BOT_SCHEMA_VERSION {
        return unavailable(mutation, "带单协议版本不匹配");
    }
    if mutation {
        ClientEvent::LeaderBotMutationApplied(access)
    } else {
        ClientEvent::LeaderBotAccess(access)
    }
}
