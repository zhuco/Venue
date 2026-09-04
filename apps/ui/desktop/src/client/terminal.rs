#[cfg(not(target_arch = "wasm32"))]
use super::{ClientEvent, REQUEST_TIMEOUT, path, publish};
#[cfg(not(target_arch = "wasm32"))]
use venue_control_protocol::kol::{
    ExecutorCommandSummary, KOL_TERMINAL_CANCEL_PATH, KOL_TERMINAL_ORDER_PATH,
    TerminalCancelRequest, TerminalOrderRequest,
};
#[cfg(not(target_arch = "wasm32"))]
use venue_control_protocol::terminal_position::{
    TERMINAL_POSITION_ACTION_PATH, TerminalPositionActionRequest,
};

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn start_native(
    client: reqwest::Client,
    endpoint: String,
    sender: crossbeam_channel::Sender<ClientEvent>,
    context: eframe::egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    orders: crossbeam_channel::Receiver<TerminalOrderRequest>,
    cancellations: crossbeam_channel::Receiver<TerminalCancelRequest>,
    positions: crossbeam_channel::Receiver<TerminalPositionActionRequest>,
) {
    tokio::spawn(async move {
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            for request in positions.try_iter().take(1) {
                if submit(
                    &client,
                    &endpoint,
                    &sender,
                    &context,
                    TERMINAL_POSITION_ACTION_PATH,
                    &request.request_id,
                    &request,
                    "持仓操作",
                )
                .await
                {
                    return;
                }
            }
            for request in orders.try_iter().take(32) {
                if submit(
                    &client,
                    &endpoint,
                    &sender,
                    &context,
                    KOL_TERMINAL_ORDER_PATH,
                    &request.request_id,
                    &request,
                    "terminal order",
                )
                .await
                {
                    return;
                }
            }
            for request in cancellations.try_iter().take(32) {
                if submit(
                    &client,
                    &endpoint,
                    &sender,
                    &context,
                    KOL_TERMINAL_CANCEL_PATH,
                    &request.request_id,
                    &request,
                    "terminal exact cancel",
                )
                .await
                {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });
}

#[cfg(not(target_arch = "wasm32"))]
async fn submit<T: serde::Serialize>(
    client: &reqwest::Client,
    endpoint: &str,
    sender: &crossbeam_channel::Sender<ClientEvent>,
    context: &eframe::egui::Context,
    route: &str,
    request_id: &str,
    request: &T,
    label: &str,
) -> bool {
    let response = client
        .post(path(endpoint, route))
        .json(request)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await;
    match response {
        Ok(response) if response.status().is_success() => {
            match response.json::<ExecutorCommandSummary>().await {
                Ok(summary)
                    if summary.validate().is_ok()
                        && summary.request_id.as_deref() == Some(request_id) =>
                {
                    publish(
                        sender,
                        context,
                        ClientEvent::TerminalExecutionUpdated(summary),
                    );
                }
                _ => publish(
                    sender,
                    context,
                    ClientEvent::TerminalSubmissionUnavailable {
                        request_id: request_id.into(),
                        definitely_not_submitted: false,
                        message: format!(
                            "{label} 回执无效，结果尚未确认；请核对历史委托，不要重复下单 [invalid_receipt]"
                        ),
                    },
                ),
            }
            false
        }
        Ok(response) if response.status().as_u16() == 401 => {
            publish(
                sender,
                context,
                ClientEvent::TerminalSubmissionUnavailable {
                    request_id: request_id.into(),
                    definitely_not_submitted: true,
                    message: crate::terminal_feedback::http_error(
                        401,
                        br#"{"code":"unauthorized"}"#,
                    ),
                },
            );
            publish(sender, context, ClientEvent::SessionExpired);
            true
        }
        Ok(response) => {
            let status = response.status().as_u16();
            let body = safe_error_body(response).await;
            publish(
                sender,
                context,
                ClientEvent::TerminalSubmissionUnavailable {
                    request_id: request_id.into(),
                    definitely_not_submitted: (400..500).contains(&status) && status != 408,
                    message: crate::terminal_feedback::http_error(status, &body),
                },
            );
            false
        }
        Err(error) => {
            publish(
                sender,
                context,
                ClientEvent::TerminalSubmissionUnavailable {
                    request_id: request_id.into(),
                    definitely_not_submitted: false,
                    message: format!(
                        "{label} {}；结果尚未确认，请核对历史委托，不要重复下单 [{}]",
                        if error.is_timeout() {
                            "请求超时"
                        } else {
                            "连接异常"
                        },
                        if error.is_timeout() {
                            "request_timeout"
                        } else {
                            "transport_unavailable"
                        }
                    ),
                },
            );
            false
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
async fn safe_error_body(response: reqwest::Response) -> Vec<u8> {
    use futures_util::StreamExt;
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(Ok(chunk)) = stream.next().await {
        if body.len().saturating_add(chunk.len()) > 8_192 {
            return Vec::new();
        }
        body.extend_from_slice(&chunk);
    }
    body
}
