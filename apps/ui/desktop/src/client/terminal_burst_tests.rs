use super::*;
use crate::account_scope::tests::{id, model};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use venue_control_protocol::kol::{TerminalAction, TerminalOrderKind};

#[tokio::test]
async fn delayed_receipts_allow_four_openings_but_fence_close_and_shutdown() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (orders, order_rx) = crossbeam_channel::unbounded();
        let (_cancels, cancel_rx) = crossbeam_channel::unbounded();
        let (_positions, position_rx) = crossbeam_channel::unbounded();
        let (events, received) = crossbeam_channel::unbounded();
        let wake = std::sync::Arc::new(tokio::sync::Notify::new());
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        start_native(reqwest::Client::builder().no_proxy().build().unwrap(), endpoint,
            events, egui::Context::default(), stop.clone(),
            NativeTerminalQueues::new(order_rx, cancel_rx, position_rx, wake.clone()));
        let scope = model().confirmed_account_scope().unwrap();
        let send = |index, action| {
            orders.send(Scoped { scope: scope.clone(), value: TerminalOrderRequest {
                schema_version: venue_control_protocol::kol::TERMINAL_SCHEMA_VERSION,
                request_id: id(index), credential_id: scope.credential_id.clone(),
                symbol: "BTC/USDC".parse().unwrap(), action,
                order_kind: TerminalOrderKind::LimitPostOnly, quote_notional: 100.into(),
                limit_price: Some(100.into()), close_quantity_cap: None,
                market_risk_confirmed: false,
            }}).unwrap();
            wake.notify_one();
        };
        let mut sockets = Vec::new();
        // Requests arriving while the first HTTP response is blocked must still be sent.
        for index in 100..104 {
            send(index, TerminalAction::OpenLong);
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 8192];
            assert!(socket.read(&mut bytes).await.unwrap() > 0);
            sockets.push(socket);
        }
        send(104, TerminalAction::CloseLong);
        assert!(tokio::time::timeout(std::time::Duration::from_millis(75), listener.accept()).await.is_err());
        for socket in &mut sockets {
            socket.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 24\r\nConnection: close\r\n\r\n{\"code\":\"invalid_input\"}").await.unwrap();
        }
        let (mut close, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 8192];
        assert!(close.read(&mut bytes).await.unwrap() > 0);
        // Stopping the worker must finish, rather than drop, the request already sent.
        stop.store(true, std::sync::atomic::Ordering::Release);
        close.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 24\r\nConnection: close\r\n\r\n{\"code\":\"invalid_input\"}").await.unwrap();
        tokio::task::spawn_blocking(move || {
            for _ in 0..5 { received.recv_timeout(std::time::Duration::from_secs(1)).unwrap(); }
        }).await.unwrap();
    }).await.unwrap();
}
