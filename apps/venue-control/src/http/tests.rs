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
    AccountDeliveryPayload, AccountDeliveryPurpose, AccountDeliveryReceipt,
    AccountDeliveryReceiptState, AccountSummary, CONTROL_SCHEMA_VERSION, CommandReceipt,
    ConnectionState, ControlAction, ControlCommandRequest, ControlEvent, ControlSnapshot,
    GatewayMode, HealthState, INDICATOR_EVENT_STREAM_PATH, INDICATOR_SNAPSHOT_PATH,
    IndicatorBinding, IndicatorFeatureValues, IndicatorFrameProjection, IndicatorProvenance,
    StrategyKind, StrategyLifecycle, StrategySummary, VenueId,
};

use super::*;
use crate::{
    AccountDeliveryRepository, AccountDeliveryRepositoryError, AccountNodeBinding, ClaimedCommand,
    CommandEnqueueResult, CommandSettleResult, ControlRepository, DeliveryStoreResult,
    IndicatorProjectionStore, RepositoryError, ScopedCommandReceipt, SnapshotStoreResult,
    StoredEvent,
};

#[derive(Clone, Default)]
struct TestRepository {
    state: Arc<Mutex<TestState>>,
}

impl AccountDeliveryRepository for TestRepository {
    async fn claim_account_deliveries(
        &self,
        binding: &AccountDeliveryBinding,
        node_id: &str,
        leased_at_ms: u64,
        expires_at_ms: u64,
        limit: u32,
    ) -> Result<Vec<AccountDeliveryClaim>, AccountDeliveryRepositoryError> {
        let (template, delay) = {
            let mut state = self.lock_delivery()?;
            state.delivery.last_claim = Some(ClaimObservation {
                binding: binding.clone(),
                node_id: node_id.to_owned(),
                leased_at_ms,
                expires_at_ms,
                limit,
            });
            (state.delivery.claim.clone(), state.delivery.claim_delay)
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let Some(template) = template else {
            return Ok(Vec::new());
        };
        Ok(vec![AccountDeliveryClaim {
            lease: AccountDeliveryLease {
                schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
                delivery_id: template.delivery_id,
                binding: binding.clone(),
                node_id: if template.mismatched_node {
                    "different-node".to_owned()
                } else {
                    node_id.to_owned()
                },
                lease_epoch: template.lease_epoch,
                leased_at_ms,
                expires_at_ms,
                purpose: template.purpose,
            },
            payload: template.payload,
        }])
    }

    async fn acknowledge_account_delivery(
        &self,
        ack: &AccountDeliveryAck,
    ) -> Result<DeliveryStoreResult, AccountDeliveryRepositoryError> {
        let mut state = self.lock_delivery()?;
        if let Some(error) = state.delivery.ack_error {
            return Err(error);
        }
        state.delivery.acks.push(ack.clone());
        Ok(DeliveryStoreResult::Stored)
    }

    async fn record_account_delivery_receipt(
        &self,
        receipt: &AccountDeliveryReceipt,
    ) -> Result<DeliveryStoreResult, AccountDeliveryRepositoryError> {
        let mut state = self.lock_delivery()?;
        if let Some(error) = state.delivery.receipt_error {
            return Err(error);
        }
        state.delivery.receipts.push(receipt.clone());
        Ok(DeliveryStoreResult::Stored)
    }
}

#[derive(Default)]
struct TestState {
    snapshot: Option<ControlSnapshot>,
    events: Vec<StoredEvent>,
    commands: BTreeMap<String, (ControlCommandRequest, CommandReceipt)>,
    delivery: DeliveryTestState,
}

#[derive(Clone)]
struct ClaimTemplate {
    delivery_id: String,
    lease_epoch: u64,
    purpose: AccountDeliveryPurpose,
    payload: AccountDeliveryPayload,
    mismatched_node: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClaimObservation {
    binding: AccountDeliveryBinding,
    node_id: String,
    leased_at_ms: u64,
    expires_at_ms: u64,
    limit: u32,
}

#[derive(Default)]
struct DeliveryTestState {
    claim: Option<ClaimTemplate>,
    claim_delay: Duration,
    last_claim: Option<ClaimObservation>,
    ack_error: Option<AccountDeliveryRepositoryError>,
    receipt_error: Option<AccountDeliveryRepositoryError>,
    acks: Vec<AccountDeliveryAck>,
    receipts: Vec<AccountDeliveryReceipt>,
}

impl TestRepository {
    fn with_snapshot(snapshot: Option<ControlSnapshot>, events: Vec<StoredEvent>) -> Self {
        Self {
            state: Arc::new(Mutex::new(TestState {
                snapshot,
                events,
                commands: BTreeMap::new(),
                delivery: DeliveryTestState::default(),
            })),
        }
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, TestState>, RepositoryError> {
        self.state.lock().map_err(|_| RepositoryError::Database)
    }

    fn lock_delivery(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, TestState>, AccountDeliveryRepositoryError> {
        self.state
            .lock()
            .map_err(|_| AccountDeliveryRepositoryError::Database)
    }

    fn set_claim(&self, claim: ClaimTemplate) -> Result<(), AccountDeliveryRepositoryError> {
        self.lock_delivery()?.delivery.claim = Some(claim);
        Ok(())
    }

    fn set_ack_error(
        &self,
        error: AccountDeliveryRepositoryError,
    ) -> Result<(), AccountDeliveryRepositoryError> {
        self.lock_delivery()?.delivery.ack_error = Some(error);
        Ok(())
    }

    fn set_claim_delay(&self, delay: Duration) -> Result<(), AccountDeliveryRepositoryError> {
        self.lock_delivery()?.delivery.claim_delay = delay;
        Ok(())
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
async fn indicator_snapshot_and_sse_are_read_only_bounded_cursor_projections()
-> Result<(), Box<dyn std::error::Error>> {
    let indicators = Arc::new(IndicatorProjectionStore::default());
    indicators.publish(indicator_projection(100)?).await?;
    let (address, stop, task) = start_with_indicators(
        TestRepository::with_snapshot(Some(snapshot()?), Vec::new()),
        Arc::clone(&indicators),
        ControlHttpConfig {
            event_poll_interval: Duration::from_millis(1),
            ..ControlHttpConfig::default()
        },
    )
    .await?;
    let snapshot_response = request(
        address,
        format!("GET {INDICATOR_SNAPSHOT_PATH} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes(),
    )
    .await?;
    assert!(snapshot_response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response_body(&snapshot_response)?.contains("\"frames\""));

    let mut stream = tokio::net::TcpStream::connect(address).await?;
    stream
        .write_all(
            format!(
                "GET {INDICATOR_EVENT_STREAM_PATH}?after=0 HTTP/1.1\r\nHost: localhost\r\nLast-Event-ID: 0\r\n\r\n"
            )
            .as_bytes(),
        )
        .await?;
    let mut received = Vec::new();
    while !String::from_utf8_lossy(&received).contains("event: indicator") {
        let mut chunk = [0_u8; 1_024];
        let count = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut chunk)).await??;
        assert_ne!(count, 0);
        received.extend_from_slice(&chunk[..count]);
    }
    assert!(String::from_utf8(received)?.contains("id: 1"));
    stop_server(stop, task).await
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
    let echoed_ack: AccountDeliveryAck = serde_json::from_str(response_body(&response)?)?;
    assert_eq!(echoed_ack, ack);
    assert!(!echoed_ack.grants_mutation_authority());

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
    let echoed_receipt: AccountDeliveryReceipt = serde_json::from_str(response_body(&response)?)?;
    assert_eq!(echoed_receipt, receipt);
    assert!(!echoed_receipt.grants_mutation_authority());
    stop_server(stop, task).await?;
    Ok(())
}

#[tokio::test]
async fn account_node_poll_revalidates_exact_node_binding_and_lease_window()
-> Result<(), Box<dyn std::error::Error>> {
    let repository = TestRepository::with_snapshot(Some(snapshot()?), Vec::new());
    let binding = delivery_binding()?;
    repository.set_claim(ClaimTemplate {
        delivery_id: "command:request-1".to_owned(),
        lease_epoch: 4,
        purpose: AccountDeliveryPurpose::Install,
        payload: AccountDeliveryPayload::ControlCommand(command(ControlAction::Pause)?),
        mismatched_node: false,
    })?;
    let (address, stop, task) = start(repository.clone(), ControlHttpConfig::default()).await?;
    let request_body = AccountDeliveryClaimRequest {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        binding: binding.clone(),
        node_id: "node-instance-a".to_owned(),
        lease_duration_ms: 2_000,
        limit: 1,
    };
    let response = request(
        address,
        &post_path(
            "/v2/account-node/deliveries/claim",
            &serde_json::to_vec(&request_body)?,
        ),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    let claims: Vec<AccountDeliveryClaim> = serde_json::from_str(response_body(&response)?)?;
    assert_eq!(claims.len(), 1);
    let claim = claims.first().ok_or("missing account delivery claim")?;
    assert_eq!(claim.lease.binding, binding);
    assert_eq!(claim.lease.node_id, "node-instance-a");
    assert_eq!(claim.lease.lease_epoch, 4);
    assert_eq!(claim.lease.purpose, AccountDeliveryPurpose::Install);
    assert_eq!(claim.lease.expires_at_ms - claim.lease.leased_at_ms, 2_000);
    assert!(!claim.grants_mutation_authority());
    let observation = repository
        .lock_delivery()?
        .delivery
        .last_claim
        .clone()
        .ok_or("claim was not passed to the repository")?;
    assert_eq!(observation.binding, binding);
    assert_eq!(observation.node_id, "node-instance-a");
    assert_eq!(observation.limit, 1);
    assert_eq!(observation.expires_at_ms - observation.leased_at_ms, 2_000);
    stop_server(stop, task).await
}

#[tokio::test]
async fn unknown_receipt_can_only_return_as_the_next_read_only_claim()
-> Result<(), Box<dyn std::error::Error>> {
    let repository = TestRepository::with_snapshot(Some(snapshot()?), Vec::new());
    let binding = delivery_binding()?;
    let install_lease = AccountDeliveryLease {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        delivery_id: "command:request-1".to_owned(),
        binding: binding.clone(),
        node_id: "node-instance-a".to_owned(),
        lease_epoch: 1,
        leased_at_ms: 100,
        expires_at_ms: 200,
        purpose: AccountDeliveryPurpose::Install,
    };
    let unknown = AccountDeliveryReceipt {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease: install_lease,
        receipt_id: "receipt-unknown-1".to_owned(),
        state: AccountDeliveryReceiptState::Unknown,
        observed_ms: 150,
        account_fact_digest: [0; 32],
        detail: "outcome requires signed account readback".to_owned(),
    };
    let (address, stop, task) = start(repository.clone(), ControlHttpConfig::default()).await?;
    let response = request(
        address,
        &post_path(
            "/v2/account-node/deliveries/receipts",
            &serde_json::to_vec(&unknown)?,
        ),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert_eq!(repository.lock_delivery()?.delivery.receipts, [unknown]);

    repository.set_claim(ClaimTemplate {
        delivery_id: "command:request-1".to_owned(),
        lease_epoch: 2,
        purpose: AccountDeliveryPurpose::ReconcileOnly,
        payload: AccountDeliveryPayload::ControlCommand(command(ControlAction::Pause)?),
        mismatched_node: false,
    })?;
    let poll = AccountDeliveryClaimRequest {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        binding,
        node_id: "node-instance-a".to_owned(),
        lease_duration_ms: 1_000,
        limit: 1,
    };
    let response = request(
        address,
        &post_path(
            "/v2/account-node/deliveries/claim",
            &serde_json::to_vec(&poll)?,
        ),
    )
    .await?;
    let claims: Vec<AccountDeliveryClaim> = serde_json::from_str(response_body(&response)?)?;
    let claim = claims.first().ok_or("missing reconciliation claim")?;
    assert_eq!(claim.lease.lease_epoch, 2);
    assert_eq!(claim.lease.purpose, AccountDeliveryPurpose::ReconcileOnly);
    assert!(!claim.grants_mutation_authority());
    stop_server(stop, task).await
}

#[tokio::test]
async fn account_node_boundary_fails_closed_on_conflict_or_mismatched_repository_claim()
-> Result<(), Box<dyn std::error::Error>> {
    let repository = TestRepository::with_snapshot(Some(snapshot()?), Vec::new());
    repository.set_claim(ClaimTemplate {
        delivery_id: "command:request-1".to_owned(),
        lease_epoch: 1,
        purpose: AccountDeliveryPurpose::Install,
        payload: AccountDeliveryPayload::ControlCommand(command(ControlAction::Pause)?),
        mismatched_node: true,
    })?;
    let poll = AccountDeliveryClaimRequest {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        binding: delivery_binding()?,
        node_id: "node-instance-a".to_owned(),
        lease_duration_ms: 1_000,
        limit: 1,
    };
    let (address, stop, task) = start(repository, ControlHttpConfig::default()).await?;
    let response = request(
        address,
        &post_path(
            "/v2/account-node/deliveries/claim",
            &serde_json::to_vec(&poll)?,
        ),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 500 Internal Server Error\r\n"));
    stop_server(stop, task).await?;

    let repository = TestRepository::with_snapshot(Some(snapshot()?), Vec::new());
    repository.set_ack_error(AccountDeliveryRepositoryError::AckConflict)?;
    let ack = AccountDeliveryAck {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        lease: AccountDeliveryLease {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            delivery_id: "command:request-1".to_owned(),
            binding: delivery_binding()?,
            node_id: "node-instance-a".to_owned(),
            lease_epoch: 1,
            leased_at_ms: 100,
            expires_at_ms: 200,
            purpose: AccountDeliveryPurpose::Install,
        },
        acknowledged_ms: 150,
        durable_inbox_digest: [7; 32],
    };
    let (address, stop, task) = start(repository, ControlHttpConfig::default()).await?;
    let response = request(
        address,
        &post_path(
            "/v2/account-node/deliveries/ack",
            &serde_json::to_vec(&ack)?,
        ),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 409 Conflict\r\n"));
    assert!(response.ends_with("{\"error\":\"delivery_conflict\"}"));
    stop_server(stop, task).await
}

#[tokio::test]
async fn account_node_boundary_caps_body_and_repository_time()
-> Result<(), Box<dyn std::error::Error>> {
    let repository = TestRepository::with_snapshot(Some(snapshot()?), Vec::new());
    let (address, stop, task) = start(
        repository,
        ControlHttpConfig {
            request_body_limit: crate::MAX_ACCOUNT_NODE_HTTP_BODY_BYTES + 1,
            ..ControlHttpConfig::default()
        },
    )
    .await?;
    let oversized = vec![b' '; crate::MAX_ACCOUNT_NODE_HTTP_BODY_BYTES + 1];
    let response = request(
        address,
        &post_path("/v2/account-node/deliveries/claim", &oversized),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 413 Payload Too Large\r\n"));
    stop_server(stop, task).await?;

    let repository = TestRepository::with_snapshot(Some(snapshot()?), Vec::new());
    repository.set_claim_delay(Duration::from_millis(50))?;
    let poll = AccountDeliveryClaimRequest {
        schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
        binding: delivery_binding()?,
        node_id: "node-instance-a".to_owned(),
        lease_duration_ms: 1_000,
        limit: 1,
    };
    let (address, stop, task) = start(
        repository,
        ControlHttpConfig {
            request_timeout: Duration::from_millis(5),
            ..ControlHttpConfig::default()
        },
    )
    .await?;
    let response = request(
        address,
        &post_path(
            "/v2/account-node/deliveries/claim",
            &serde_json::to_vec(&poll)?,
        ),
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 504 Gateway Timeout\r\n"));
    stop_server(stop, task).await
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
    let task = tokio::spawn(serve_inner(
        listener,
        Arc::new(ControlService::new(repository)),
        Arc::new(IndicatorProjectionStore::default()),
        config,
        shutdown,
        AccessMode::TransportFixture,
    ));
    Ok((address, stop, task))
}

async fn start_with_indicators(
    repository: TestRepository,
    indicators: Arc<IndicatorProjectionStore>,
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
    let task = tokio::spawn(serve_inner(
        listener,
        Arc::new(ControlService::new(repository)),
        indicators,
        config,
        shutdown,
        AccessMode::TransportFixture,
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

fn response_body(response: &str) -> Result<&str, Box<dyn std::error::Error>> {
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .ok_or_else(|| "HTTP response is missing its header delimiter".into())
}

fn delivery_binding() -> Result<AccountDeliveryBinding, Box<dyn std::error::Error>> {
    Ok(AccountDeliveryBinding {
        venue: VenueId::Binance,
        mode: GatewayMode::Live,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        symbol: "BTC/USDT".parse()?,
        instance_id: "grid-btc".to_owned(),
        config_epoch: 7,
    })
}

fn indicator_projection(
    observed_ms: u64,
) -> Result<IndicatorFrameProjection, Box<dyn std::error::Error>> {
    let event_time_ms = observed_ms.checked_sub(1).ok_or("indicator time")?;
    Ok(IndicatorFrameProjection {
        schema_version: CONTROL_SCHEMA_VERSION,
        binding: IndicatorBinding {
            venue: VenueId::Binance,
            mode: GatewayMode::Live,
            trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            symbol: "BTC/USDT".parse()?,
        },
        generation: 7,
        watermark_ms: event_time_ms,
        observed_ms,
        maximum_age_ms: 100,
        provenance: ["book", "trades", "bars"]
            .into_iter()
            .map(|source| IndicatorProvenance {
                source: source.to_owned(),
                generation: 7,
                sequence: 1,
                event_time_ms,
                age_ms: 1,
                feature_version: "v1".to_owned(),
            })
            .collect(),
        values: IndicatorFeatureValues {
            mid_price: Decimal::from(100),
            fair_price: Decimal::from(100),
            spread_bps: Decimal::ONE,
            depth_quote: Decimal::from(1_000),
            book_imbalance: Decimal::ZERO,
            trade_imbalance: Decimal::ZERO,
            short_return_bps: Decimal::ZERO,
            trend_efficiency: Decimal::ZERO,
            bandwidth_expansion: Decimal::ZERO,
            expected_move_bps: Decimal::ONE,
            toxicity: Decimal::ZERO,
        },
    })
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

pub(super) fn command(
    action: ControlAction,
) -> Result<ControlCommandRequest, Box<dyn std::error::Error>> {
    Ok(ControlCommandRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: "request-1".to_owned(),
        venue: VenueId::Binance,
        mode: GatewayMode::Live,
        trading_account_id: "00000000-0000-4000-8000-000000000001".to_owned(),
        instance_id: "grid-btc".to_owned(),
        symbol: "BTC/USDT".parse()?,
        action,
        trade: None,
        expected_config_epoch: 7,
        confirmation: None,
    })
}

pub(super) fn snapshot() -> Result<ControlSnapshot, Box<dyn std::error::Error>> {
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
