#[cfg(not(target_arch = "wasm32"))]
use super::{ClientEvent, REQUEST_TIMEOUT, path, publish};
use crate::account_scope::{AccountScope, Scoped};
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
pub(super) struct NativeTerminalQueues {
    orders: crossbeam_channel::Receiver<Scoped<TerminalOrderRequest>>,
    cancellations: crossbeam_channel::Receiver<Scoped<TerminalCancelRequest>>,
    positions: crossbeam_channel::Receiver<Scoped<TerminalPositionActionRequest>>,
    wake: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeTerminalQueues {
    pub(super) fn new(
        orders: crossbeam_channel::Receiver<Scoped<TerminalOrderRequest>>,
        cancellations: crossbeam_channel::Receiver<Scoped<TerminalCancelRequest>>,
        positions: crossbeam_channel::Receiver<Scoped<TerminalPositionActionRequest>>,
        wake: std::sync::Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            orders,
            cancellations,
            positions,
            wake,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeTerminalSubmitter {
    client: reqwest::Client,
    endpoint: String,
    sender: crossbeam_channel::Sender<ClientEvent>,
    context: eframe::egui::Context,
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn start_native(
    client: reqwest::Client,
    endpoint: String,
    sender: crossbeam_channel::Sender<ClientEvent>,
    context: eframe::egui::Context,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    queues: NativeTerminalQueues,
) {
    let submitter = NativeTerminalSubmitter {
        client,
        endpoint,
        sender,
        context,
    };
    tokio::spawn(async move {
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
            for Scoped {
                scope,
                value: request,
            } in queues.positions.try_iter().take(1)
            {
                if submitter
                    .submit(
                        TERMINAL_POSITION_ACTION_PATH,
                        &scope,
                        &request.request_id,
                        &request,
                        "持仓操作",
                    )
                    .await
                {
                    return;
                }
            }
            for Scoped {
                scope,
                value: request,
            } in queues.orders.try_iter().take(32)
            {
                if submitter
                    .submit(
                        KOL_TERMINAL_ORDER_PATH,
                        &scope,
                        &request.request_id,
                        &request,
                        "terminal order",
                    )
                    .await
                {
                    return;
                }
            }
            for Scoped {
                scope,
                value: request,
            } in queues.cancellations.try_iter().take(32)
            {
                if submitter
                    .submit(
                        KOL_TERMINAL_CANCEL_PATH,
                        &scope,
                        &request.request_id,
                        &request,
                        "terminal exact cancel",
                    )
                    .await
                {
                    return;
                }
            }
            tokio::select! {
                _ = queues.wake.notified() => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {},
            }
        }
    });
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeTerminalSubmitter {
    async fn submit<T: serde::Serialize>(
        &self,
        route: &str,
        scope: &AccountScope,
        request_id: &str,
        request: &T,
        label: &str,
    ) -> bool {
        let response = self
            .client
            .post(path(&self.endpoint, route))
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
                        publish_scoped(
                            scope,
                            &self.sender,
                            &self.context,
                            ClientEvent::TerminalExecutionUpdated(summary),
                        );
                    }
                    _ => publish_scoped(
                        scope,
                        &self.sender,
                        &self.context,
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
                publish_scoped(
                    scope,
                    &self.sender,
                    &self.context,
                    ClientEvent::TerminalSubmissionUnavailable {
                        request_id: request_id.into(),
                        definitely_not_submitted: true,
                        message: crate::terminal_feedback::http_error(
                            401,
                            br#"{"code":"unauthorized"}"#,
                        ),
                    },
                );
                publish_scoped(
                    scope,
                    &self.sender,
                    &self.context,
                    ClientEvent::SessionExpired,
                );
                false
            }
            Ok(response) => {
                let status = response.status().as_u16();
                let body = safe_error_body(response).await;
                publish_scoped(
                    scope,
                    &self.sender,
                    &self.context,
                    ClientEvent::TerminalSubmissionUnavailable {
                        request_id: request_id.into(),
                        definitely_not_submitted: (400..500).contains(&status) && status != 408,
                        message: crate::terminal_feedback::http_error(status, &body),
                    },
                );
                false
            }
            Err(error) => {
                publish_scoped(
                    scope,
                    &self.sender,
                    &self.context,
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

#[cfg(not(target_arch = "wasm32"))]
fn publish_scoped(
    scope: &AccountScope,
    sender: &crossbeam_channel::Sender<ClientEvent>,
    context: &eframe::egui::Context,
    event: ClientEvent,
) {
    publish(sender, context, scope.event(event));
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod race_tests {
    use super::*;
    use crate::account_scope::tests::{id, model, overview, receipt};
    #[tokio::test]
    async fn selection_http_barrier_submit_cancel_position_receipts_are_ui_scoped() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for route in [
                KOL_TERMINAL_ORDER_PATH,
                KOL_TERMINAL_CANCEL_PATH,
                TERMINAL_POSITION_ACTION_PATH,
            ] {
                let mut model = model();
                let scope = model.confirmed_account_scope().unwrap();
                let (endpoint, seen, release, server) =
                    crate::client::execution::race_tests::barrier_server(
                        200,
                        serde_json::to_string(&receipt(1)).unwrap(),
                    )
                    .await;
                let (sender, events) = crossbeam_channel::unbounded();
                let submitter = NativeTerminalSubmitter {
                    client: reqwest::Client::builder().no_proxy().build().unwrap(),
                    endpoint,
                    sender,
                    context: egui::Context::default(),
                };
                let worker = tokio::spawn(async move {
                    submitter
                        .submit(
                            route,
                            &scope,
                            &id(81),
                            &serde_json::json!({"request_id": id(81)}),
                            "fixture",
                        )
                        .await
                });
                seen.await.unwrap();
                model.begin_account_selection(id(2));
                model.apply_account_overview(overview(2));
                release.send(()).unwrap();
                assert!(!worker.await.unwrap());
                let ClientEvent::AccountScoped { scope, event } = events.try_recv().unwrap() else {
                    panic!("untagged receipt");
                };
                assert!(matches!(*event, ClientEvent::TerminalExecutionUpdated(_)));
                assert!(!model.accept_account_event(&scope, &event));
                model.apply_account_event(&scope, *event);
                assert!(model.execution.terminal_executions.is_empty());
                assert!(
                    model.execution.terminal_submission_error.is_none() && model.notices.is_empty()
                );
                server.await.unwrap();
            }
        })
        .await
        .unwrap();
    }
}
