#[cfg(not(target_arch = "wasm32"))]
use super::{ClientEvent, REQUEST_TIMEOUT, path, publish};
#[cfg(not(target_arch = "wasm32"))]
use venue_control_protocol::kol::{
    ExecutorCommandSummary, KOL_TERMINAL_CANCEL_PATH, KOL_TERMINAL_ORDER_PATH,
    TerminalCancelRequest, TerminalOrderRequest,
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
) {
    tokio::spawn(async move {
        while !stop.load(std::sync::atomic::Ordering::Acquire) {
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
                        ClientEvent::TerminalExecutions(vec![summary]),
                    );
                }
                _ => publish(
                    sender,
                    context,
                    ClientEvent::CommandUnavailable(format!("invalid {label} command receipt")),
                ),
            }
            false
        }
        Ok(response) if response.status().as_u16() == 401 => {
            publish(sender, context, ClientEvent::SessionExpired);
            true
        }
        Ok(response) => {
            publish(
                sender,
                context,
                ClientEvent::CommandUnavailable(format!(
                    "{label} returned HTTP {}",
                    response.status()
                )),
            );
            false
        }
        Err(error) => {
            publish(
                sender,
                context,
                ClientEvent::CommandUnavailable(format!("{label} failed: {error}")),
            );
            false
        }
    }
}
