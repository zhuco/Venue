use super::*;
use crate::account_scope::tests::{id, model, overview, projection, receipt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// The server cannot reply until the test has observed the complete HTTP request
/// and changed the UI selection. Timeouts only bound failures; no timing sleeps.
pub(crate) async fn barrier_server(
    status: u16,
    body: String,
) -> (
    String,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = vec![];
        loop {
            let mut buf = [0; 2048];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            bytes.extend_from_slice(&buf[..n]);
            if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                if bytes.len() >= end + 4 + length {
                    break;
                }
            }
        }
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let response = format!(
            "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    (endpoint, seen_rx, release_tx, server)
}

#[tokio::test]
async fn selection_http_barrier_projection_success_empty_unavailable_401_and_aba() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for (status, body) in [
            (200, serde_json::to_string(&Some(projection(1))).unwrap()),
            (200, "null".into()),
            (503, "{}".into()),
            (401, "{}".into()),
        ] {
            for again in [false, true] {
                let mut model = model();
                let old = model.confirmed_account_scope().unwrap();
                let request = Scoped {
                    scope: old.clone(),
                    value: TerminalProjectionRequest {
                        schema_version: 1,
                        credential_id: id(1),
                        symbols: vec!["BTC/USDC".parse().unwrap()],
                    },
                };
                let (endpoint, seen, release, server) = barrier_server(status, body.clone()).await;
                let worker = tokio::spawn(async move {
                    projection_read(
                        &reqwest::Client::builder().no_proxy().build().unwrap(),
                        &endpoint,
                        &request,
                    )
                    .await
                });
                seen.await.unwrap();
                model.begin_account_selection(id(2));
                model.apply_account_overview(overview(2));
                if again {
                    model.begin_account_selection(id(1));
                    model.apply_account_overview(overview(1));
                }
                release.send(()).unwrap();
                let ClientEvent::AccountScoped { scope, event } = worker.await.unwrap() else {
                    panic!("untagged HTTP result");
                };
                assert_eq!(scope, old);
                assert!(!model.accept_account_event(&scope, &event));
                assert!(!model.apply_account_event(&scope, *event));
                assert!(
                    model.execution.private_projection.is_none()
                        && model.execution.private_error.is_none()
                );
                assert!(model.notices.is_empty());
                server.await.unwrap();
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn selection_http_barrier_history_cannot_update_new_account() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut model = model();
        let old = model.confirmed_account_scope().unwrap();
        let (endpoint, seen, release, server) =
            barrier_server(200, serde_json::to_string(&vec![receipt(1)]).unwrap()).await;
        let worker = tokio::spawn(async move {
            history_read(
                &reqwest::Client::builder().no_proxy().build().unwrap(),
                &endpoint,
                &old,
            )
            .await
        });
        seen.await.unwrap();
        model.begin_account_selection(id(2));
        model.apply_account_overview(overview(2));
        release.send(()).unwrap();
        let ClientEvent::AccountScoped { scope, event } = worker.await.unwrap() else {
            panic!("untagged history");
        };
        assert!(!model.accept_account_event(&scope, &event));
        model.apply_account_event(&scope, *event);
        assert!(model.execution.terminal_executions.is_empty());
        server.await.unwrap();
    })
    .await
    .unwrap();
}
