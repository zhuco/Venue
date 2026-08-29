use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use rust_decimal::Decimal;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryAck, AccountDeliveryBinding,
    AccountDeliveryClaim, AccountDeliveryClaimRequest, AccountDeliveryLease,
    AccountDeliveryPurpose, AccountDeliveryReceipt, AccountDeliveryReceiptState, AccountSummary,
    CONTROL_SCHEMA_VERSION, CommandReceipt, ConnectionState, ControlAction, ControlCommandRequest,
    ControlEvent, ControlSnapshot, GatewayMode, HealthState, StrategyKind, StrategyLifecycle,
    StrategySummary, VenueId,
};

use super::*;
use crate::{
    AccountDeliveryRepository, AccountDeliveryRepositoryError, AccountNodeBinding, ClaimedCommand,
    CommandEnqueueResult, CommandSettleResult, ControlRepository, DeliveryStoreResult,
    RepositoryError, ScopedCommandReceipt, SnapshotStoreResult, StoredEvent,
};

#[derive(Clone, Default)]
struct TestRepository {
    state: Arc<Mutex<TestState>>,
}

impl AccountDeliveryRepository for TestRepository {
    async fn claim_account_deliveries(
        &self,
        _: &AccountDeliveryBinding,
        _: &str,
        _: u64,
        _: u64,
        _: u32,
    ) -> Result<Vec<AccountDeliveryClaim>, AccountDeliveryRepositoryError> {
        Ok(Vec::new())
    }

    async fn acknowledge_account_delivery(
        &self,
        _: &AccountDeliveryAck,
    ) -> Result<DeliveryStoreResult, AccountDeliveryRepositoryError> {
        Ok(DeliveryStoreResult::Stored)
    }

    async fn record_account_delivery_receipt(
        &self,
        _: &AccountDeliveryReceipt,
    ) -> Result<DeliveryStoreResult, AccountDeliveryRepositoryError> {
        Ok(DeliveryStoreResult::Stored)
    }
}

#[derive(Default)]
struct TestState {
    snapshot: Option<ControlSnapshot>,
    events: Vec<StoredEvent>,
    commands: BTreeMap<String, (ControlCommandRequest, CommandReceipt)>,
}

impl TestRepository {
    fn with_snapshot(snapshot: Option<ControlSnapshot>, events: Vec<StoredEvent>) -> Self {
        Self {
            state: Arc::new(Mutex::new(TestState {
                snapshot,
                events,
                commands: BTreeMap::new(),
            })),
        }
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, TestState>, RepositoryError> {
        self.state.lock().map_err(|_| RepositoryError::Database)
    }
}

impl ControlRepository for TestRepository {
    async fn load_snapshot(&self) -> Result<Option<ControlSnapshot>, RepositoryError> {
        Ok(self.lock()?.snapshot.clone())
    }
    async fn store_snapshot(
        &self,
        snapshot: &ControlSnapshot,
    ) -> Result<SnapshotStoreResult, RepositoryError> {
        let mut state = self.lock()?;
        state.snapshot = Some(snapshot.clone());
        let sequence = state.events.len() as i64 + 1;
        state.events.push(StoredEvent {
            sequence,
            event: ControlEvent::Snapshot(snapshot.clone()),
        });
        Ok(SnapshotStoreResult::Inserted {
            event_sequence: sequence,
        })
    }
    async fn enqueue_command(
        &self,
        command: &ControlCommandRequest,
        accepted: &CommandReceipt,
    ) -> Result<CommandEnqueueResult, RepositoryError> {
        let mut state = self.lock()?;
        if !has_scope(state.snapshot.as_ref(), command) {
            return Err(RepositoryError::StaleScope);
        }
        if let Some((current, receipt)) = state.commands.get(&command.request_id) {
            return if current == command {
                Ok(CommandEnqueueResult::Existing(receipt.clone()))
            } else {
                Err(RepositoryError::ReplayConflict)
            };
        }
        state.commands.insert(
            command.request_id.clone(),
            (command.clone(), accepted.clone()),
        );
        let sequence = state.events.len() as i64 + 1;
        state.events.push(StoredEvent {
            sequence,
            event: ControlEvent::CommandReceipt(accepted.clone()),
        });
        Ok(CommandEnqueueResult::Inserted(accepted.clone()))
    }
    async fn claim_commands(
        &self,
        _: &AccountNodeBinding,
        _: &str,
        _: u64,
        _: u32,
    ) -> Result<Vec<ClaimedCommand>, RepositoryError> {
        Ok(Vec::new())
    }
    async fn settle_command(
        &self,
        _: &ScopedCommandReceipt,
    ) -> Result<CommandSettleResult, RepositoryError> {
        Err(RepositoryError::DeliveryConflict)
    }
    async fn list_events(
        &self,
        after: i64,
        limit: u32,
    ) -> Result<Vec<StoredEvent>, RepositoryError> {
        Ok(self
            .lock()?
            .events
            .iter()
            .filter(|event| event.sequence > after)
            .take(limit as usize)
            .cloned()
            .collect())
    }
    async fn has_current_strategy_scope(
        &self,
        command: &ControlCommandRequest,
    ) -> Result<bool, RepositoryError> {
        Ok(has_scope(self.lock()?.snapshot.as_ref(), command))
    }
    async fn has_current_account_scope(
        &self,
        venue: VenueId,
        mode: GatewayMode,
        account: &str,
    ) -> Result<bool, RepositoryError> {
        Ok(self.lock()?.snapshot.as_ref().is_some_and(|snapshot| {
            snapshot.accounts.iter().any(|item| {
                item.venue == venue && item.mode == mode && item.trading_account_id == account
            })
        }))
    }
}

#[tokio::test]
async fn snapshot_command_limits_and_error_mapping_use_exact_http_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let (address, stop, task) = start(
        TestRepository::with_snapshot(Some(snapshot()?), Vec::new()),
        ControlHttpConfig::default(),
    )
    .await?;
    let snapshot_response = request(
        address,
        b"GET /v2/ui/snapshot HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await?;
    assert!(snapshot_response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(snapshot_response.contains("Connection: close\r\n"));
    let command = serde_json::to_vec(&command(ControlAction::Pause)?)?;
    let command_request = post_request(&command);
    let first = request(address, &command_request).await?;
    assert!(first.starts_with("HTTP/1.1 200 OK\r\n"));
    let replay = request(address, &command_request).await?;
    assert_eq!(first, replay);
    let oversized = request(
        address,
        b"POST /v2/control/commands HTTP/1.1\r\nHost: localhost\r\nContent-Length: 65537\r\n\r\n",
    )
    .await?;
    assert!(oversized.starts_with("HTTP/1.1 413 Payload Too Large\r\n"));
    let chunked = request(address, b"POST /v2/control/commands HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n").await?;
    assert!(chunked.starts_with("HTTP/1.1 400 Bad Request\r\n"));
    let pipelined = request(address, b"GET /v2/ui/snapshot HTTP/1.1\r\nHost: localhost\r\n\r\nGET /v2/ui/snapshot HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;
    assert!(pipelined.starts_with("HTTP/1.1 400 Bad Request\r\n"));
    stop_server(stop, task).await?;

    let (address, stop, task) =
        start(TestRepository::default(), ControlHttpConfig::default()).await?;
    let unavailable = request(
        address,
        b"GET /v2/ui/snapshot HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await?;
    assert!(unavailable.starts_with("HTTP/1.1 503 Service Unavailable\r\n"));
    stop_server(stop, task).await
}

#[tokio::test]
async fn sse_replays_cursor_and_gracefully_stops() -> Result<(), Box<dyn std::error::Error>> {
    let events = vec![
        StoredEvent {
            sequence: 1,
            event: ControlEvent::Notice {
                observed_ms: 1,
                message: "one".to_owned(),
            },
        },
        StoredEvent {
            sequence: 2,
            event: ControlEvent::Notice {
                observed_ms: 2,
                message: "two".to_owned(),
            },
        },
    ];
    let (address, stop, task) = start(
        TestRepository::with_snapshot(Some(snapshot()?), events),
        ControlHttpConfig {
            event_poll_interval: Duration::from_millis(1),
            ..ControlHttpConfig::default()
        },
    )
    .await?;
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    stream
        .write_all(
            b"GET /v2/ui/events?after=1 HTTP/1.1\r\nHost: localhost\r\nLast-Event-ID: 1\r\n\r\n",
        )
        .await?;
    let mut received = Vec::new();
    while !String::from_utf8_lossy(&received).contains("id: 2") {
        let mut chunk = [0_u8; 1_024];
        let count = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut chunk)).await??;
        assert_ne!(count, 0);
        received.extend_from_slice(&chunk[..count]);
    }
    assert!(String::from_utf8(received)?.contains("id: 2"));
    let _ = stop.send(true);
    let mut tail = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut tail)).await??;
    task.await??;
    Ok(())
}

#[tokio::test]
async fn request_timeout_and_slow_sse_writes_fail_closed() -> Result<(), Box<dyn std::error::Error>>
{
    let (address, stop, task) = start(
        TestRepository::with_snapshot(Some(snapshot()?), Vec::new()),
        ControlHttpConfig {
            request_timeout: Duration::from_millis(5),
            ..ControlHttpConfig::default()
        },
    )
    .await?;
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    stream
        .write_all(b"GET /v2/ui/snapshot HTTP/1.1\r\nHost: localhost")
        .await?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response)).await??;
    assert!(String::from_utf8(response)?.starts_with("HTTP/1.1 504 Gateway Timeout\r\n"));
    stop_server(stop, task).await?;

    let (mut writer, _reader) = tokio::io::duplex(1);
    assert!(
        write_sse(&mut writer, b"more-than-one-byte", Duration::from_millis(5))
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn account_node_delivery_http_routes_only_transport_versioned_database_requests()
-> Result<(), Box<dyn std::error::Error>> {
    let (address, stop, task) = start(
        TestRepository::with_snapshot(Some(snapshot()?), Vec::new()),
        ControlHttpConfig::default(),
    )
    .await?;
    let binding = AccountDeliveryBinding {
        venue: VenueId::Binance,
        mode: GatewayMode::Live,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        symbol: "BTC/USDT".parse()?,
        instance_id: "grid-btc".to_owned(),
        config_epoch: 7,
    };
    let claim_request = AccountDeliveryClaimRequest {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        binding: binding.clone(),
        node_id: "node-a".to_owned(),
        lease_duration_ms: 1_000,
        limit: 10,
    };
    let response = request(
        address,
        &post_path(
            "/v2/account-node/deliveries/claim",
            &serde_json::to_vec(&claim_request)?,
        ),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with("[]"));

    let lease = AccountDeliveryLease {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        delivery_id: "command:request-1".to_owned(),
        binding,
        node_id: "node-a".to_owned(),
        lease_epoch: 1,
        leased_at_ms: 100,
        expires_at_ms: 200,
        purpose: AccountDeliveryPurpose::Install,
    };
    let ack = AccountDeliveryAck {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease: lease.clone(),
        acknowledged_ms: 110,
        durable_inbox_digest: [1; 32],
    };
    let response = request(
        address,
        &post_path(
            "/v2/account-node/deliveries/ack",
            &serde_json::to_vec(&ack)?,
        ),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));

    let receipt = AccountDeliveryReceipt {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease,
        receipt_id: "receipt-1".to_owned(),
        state: AccountDeliveryReceiptState::Applied,
        observed_ms: 120,
        account_fact_digest: [2; 32],
        detail: "installed and applied by the account actor".to_owned(),
    };
    let response = request(
        address,
        &post_path(
            "/v2/account-node/deliveries/receipts",
            &serde_json::to_vec(&receipt)?,
        ),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    stop_server(stop, task).await?;
    Ok(())
}

#[tokio::test]
async fn non_loopback_listener_is_rejected_before_accepting_clients()
-> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let (_, shutdown) = control_shutdown_channel();
    let result = serve_local(
        listener,
        Arc::new(ControlService::new(TestRepository::default())),
        ControlHttpConfig::default(),
        shutdown,
    )
    .await;
    assert!(matches!(result, Err(HttpServerError::NonLoopbackBind)));
    Ok(())
}

async fn start(
    repository: TestRepository,
    config: ControlHttpConfig,
) -> Result<
    (
        std::net::SocketAddr,
        watch::Sender<bool>,
        tokio::task::JoinHandle<Result<(), HttpServerError>>,
    ),
    Box<dyn std::error::Error>,
> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, shutdown) = control_shutdown_channel();
    let task = tokio::spawn(serve_local(
        listener,
        Arc::new(ControlService::new(repository)),
        config,
        shutdown,
    ));
    Ok((address, stop, task))
}

async fn stop_server(
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<Result<(), HttpServerError>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let _ = stop.send(true);
    task.await??;
    Ok(())
}

async fn request(
    address: std::net::SocketAddr,
    request: &[u8],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = tokio::net::TcpStream::connect(address).await?;
    stream.write_all(request).await?;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), stream.read_to_end(&mut response)).await??;
    Ok(String::from_utf8(response)?)
}

fn post_request(body: &[u8]) -> Vec<u8> {
    post_path("/v2/control/commands", body)
}

fn post_path(path: &str, body: &[u8]) -> Vec<u8> {
    [
        format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes(),
        body.to_vec(),
    ]
    .concat()
}

fn has_scope(snapshot: Option<&ControlSnapshot>, command: &ControlCommandRequest) -> bool {
    snapshot.is_some_and(|snapshot| {
        snapshot.strategies.iter().any(|strategy| {
            strategy.venue == command.venue
                && strategy.mode == command.mode
                && strategy.trading_account_id == command.trading_account_id
                && strategy.symbol == command.symbol
                && strategy.instance_id == command.instance_id
                && strategy.config_epoch == command.expected_config_epoch
        })
    })
}

fn command(action: ControlAction) -> Result<ControlCommandRequest, Box<dyn std::error::Error>> {
    Ok(ControlCommandRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: "request-1".to_owned(),
        venue: VenueId::Binance,
        mode: GatewayMode::Live,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        instance_id: "grid-btc".to_owned(),
        symbol: "BTC/USDT".parse()?,
        action,
        expected_config_epoch: 7,
        confirmation: None,
    })
}

fn snapshot() -> Result<ControlSnapshot, Box<dyn std::error::Error>> {
    Ok(ControlSnapshot {
        schema_version: CONTROL_SCHEMA_VERSION,
        generated_ms: 100,
        connection: ConnectionState::Live,
        accounts: vec![AccountSummary {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            health: HealthState::Healthy,
            equity: Decimal::new(10_000, 0),
            available_margin: Decimal::new(8_000, 0),
            unrealized_pnl: Decimal::ZERO,
            private_generation: 2,
            writer_generation: 1,
            last_reconciled_ms: 99,
        }],
        strategies: vec![StrategySummary {
            instance_id: "grid-btc".to_owned(),
            kind: StrategyKind::Grid,
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
            lifecycle: StrategyLifecycle::Running,
            config_epoch: 7,
            open_orders: 4,
            long_quantity: Decimal::ONE,
            short_quantity: Decimal::ONE,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            last_receipt_ms: 99,
            attention: None,
        }],
        copy_relations: Vec::new(),
        markets: Vec::new(),
        ledger: Vec::new(),
    })
}
