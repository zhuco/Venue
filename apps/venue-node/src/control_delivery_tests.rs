use std::{
    fs::OpenOptions,
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use venue_control_protocol::{
    ACCOUNT_DELIVERY_SCHEMA_VERSION, AccountDeliveryBinding, AccountDeliveryClaim,
    AccountDeliveryLease, AccountDeliveryPayload, AccountDeliveryPurpose,
    AccountDeliveryReceiptState, CONTROL_SCHEMA_VERSION, ControlAction, ControlCommandRequest,
    GatewayMode, VenueId,
};
use venue_domain::Symbol;

use super::{
    ClaimAcceptance, ControlDeliveryDriver, ControlDeliveryError, ControlDeliveryInbox,
    ControlDeliveryJournal, ControlDeliveryJournalError, ControlDeliveryJournalRecord,
    ControlDeliveryWork, ControlHttpClient, ControlHttpClientConfig, ControlHttpClientError,
    DurableStoreResult, OpaqueControlDeliveryJournal,
};

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000032";
const NODE: &str = "venue-node-goal32";

#[derive(Clone, Default)]
struct MemoryJournal {
    records: Arc<Mutex<Vec<ControlDeliveryJournalRecord>>>,
}

impl MemoryJournal {
    fn len(&self) -> Result<usize, ControlDeliveryJournalError> {
        self.records
            .lock()
            .map(|records| records.len())
            .map_err(|_| ControlDeliveryJournalError::Unavailable)
    }
}

impl ControlDeliveryJournal for MemoryJournal {
    fn recover(
        &mut self,
    ) -> Result<Vec<ControlDeliveryJournalRecord>, ControlDeliveryJournalError> {
        self.records
            .lock()
            .map(|records| records.clone())
            .map_err(|_| ControlDeliveryJournalError::Unavailable)
    }

    fn append(
        &mut self,
        expected_sequence: u64,
        payload: &[u8],
    ) -> Result<u64, ControlDeliveryJournalError> {
        let mut records = self
            .records
            .lock()
            .map_err(|_| ControlDeliveryJournalError::Unavailable)?;
        let sequence = u64::try_from(records.len())
            .map_err(|_| ControlDeliveryJournalError::SequenceConflict)?
            .checked_add(1)
            .ok_or(ControlDeliveryJournalError::SequenceConflict)?;
        if sequence != expected_sequence {
            return Err(ControlDeliveryJournalError::SequenceConflict);
        }
        records.push(ControlDeliveryJournalRecord {
            sequence,
            payload: payload.to_vec(),
        });
        Ok(sequence)
    }
}

#[test]
fn ack_is_durable_and_control_confirmed_before_actor_applied()
-> Result<(), Box<dyn std::error::Error>> {
    let journal = MemoryJournal::default();
    let mut inbox = new_inbox(journal.clone())?;
    let claim = claim(1, 100, 200, AccountDeliveryPurpose::Install)?;

    let accepted = inbox.accept_claim(claim.clone(), 110)?;
    let ack = match accepted {
        ClaimAcceptance::Install(output) => {
            assert_eq!(output.store_result(), DurableStoreResult::Stored);
            assert_eq!(output.durable_sequence(), 2);
            assert!(!output.grants_gateway_capability());
            assert!(!output.grants_writer_lease());
            assert!(!output.grants_wal_authority());
            assert!(!output.grants_dispatch_permit());
            output.value().clone()
        }
        ClaimAcceptance::Reconcile(_) => return Err("install claim became reconciliation".into()),
    };
    assert!(inbox.actor_turn("command:request-32", 120)?.is_none());

    let duplicate = inbox.accept_claim(claim, 120)?;
    match duplicate {
        ClaimAcceptance::Install(output) => {
            assert_eq!(output.store_result(), DurableStoreResult::Existing);
            assert_eq!(output.value(), &ack);
        }
        ClaimAcceptance::Reconcile(_) => return Err("duplicate changed purpose".into()),
    }
    assert_eq!(journal.len()?, 2);

    assert_eq!(
        inbox.confirm_acknowledgement(&ack, 125)?,
        DurableStoreResult::Stored
    );
    let turn = inbox
        .actor_turn("command:request-32", 130)?
        .ok_or("actor turn missing after ACK")?;
    assert!(!turn.grants_gateway_capability());
    assert!(!turn.grants_writer_lease());
    assert!(!turn.grants_wal_authority());
    assert!(!turn.grants_dispatch_permit());
    let completion = turn.applied(140, digest(7), "actor inbox and checkpoint durable")?;
    let receipt = inbox.record_actor_completion(completion)?;
    assert_eq!(receipt.value().state, AccountDeliveryReceiptState::Applied);
    assert_eq!(receipt.durable_sequence(), 4);
    assert!(!receipt.grants_dispatch_permit());
    assert_eq!(
        inbox.confirm_receipt(receipt.value(), 150)?,
        DurableStoreResult::Stored
    );
    assert!(inbox.actor_turn("command:request-32", 160)?.is_none());

    let recovered = new_inbox(journal)?;
    assert!(recovered.pending_acknowledgements(160).is_empty());
    assert!(recovered.pending_receipts().is_empty());
    assert!(recovered.actor_turn("command:request-32", 160)?.is_none());
    Ok(())
}

#[test]
fn unknown_accepts_only_the_exact_next_reconciliation_claim()
-> Result<(), Box<dyn std::error::Error>> {
    let journal = MemoryJournal::default();
    let mut inbox = new_inbox(journal.clone())?;
    let install = claim(1, 100, 200, AccountDeliveryPurpose::Install)?;
    let ack = install_ack(&mut inbox, install, 110, 120)?;
    assert_eq!(ack.lease.lease_epoch, 1);
    let turn = inbox
        .actor_turn("command:request-32", 130)?
        .ok_or("actor turn missing")?;
    let unknown = inbox.record_actor_completion(turn.unknown(
        140,
        [0; 32],
        "exchange result cannot be proven",
    )?)?;
    assert_eq!(unknown.value().state, AccountDeliveryReceiptState::Unknown);
    inbox.confirm_receipt(unknown.value(), 150)?;

    let reconciliation = claim(2, 200, 300, AccountDeliveryPurpose::ReconcileOnly)?;
    let turn = match inbox.accept_claim(reconciliation, 210)? {
        ClaimAcceptance::Reconcile(turn) => turn,
        ClaimAcceptance::Install(_) => return Err("unknown was reinstalled".into()),
    };
    assert!(!turn.grants_gateway_capability());
    assert!(!turn.grants_writer_lease());
    assert!(!turn.grants_wal_authority());
    assert!(!turn.grants_dispatch_permit());
    let reconciled = inbox.record_reconciliation(turn.reconciled(
        220,
        digest(9),
        "signed account facts resolve the unknown",
    )?)?;
    assert_eq!(
        reconciled.value().state,
        AccountDeliveryReceiptState::Reconciled
    );
    inbox.confirm_receipt(reconciled.value(), 230)?;

    let recovered = new_inbox(journal)?;
    assert!(recovered.pending_receipts().is_empty());
    assert!(
        recovered
            .reconciliation_turn("command:request-32", 240)?
            .is_none()
    );
    Ok(())
}

#[test]
fn install_after_unknown_is_durably_failed_closed() -> Result<(), Box<dyn std::error::Error>> {
    let journal = MemoryJournal::default();
    let mut inbox = new_inbox(journal.clone())?;
    let install = claim(1, 100, 200, AccountDeliveryPurpose::Install)?;
    install_ack(&mut inbox, install, 110, 120)?;
    let turn = inbox
        .actor_turn("command:request-32", 130)?
        .ok_or("actor turn missing")?;
    let unknown = inbox.record_actor_completion(turn.unknown(140, [0; 32], "unknown")?)?;
    inbox.confirm_receipt(unknown.value(), 150)?;

    let invalid = claim(2, 200, 300, AccountDeliveryPurpose::Install)?;
    assert!(matches!(
        inbox.accept_claim(invalid, 210),
        Err(ControlDeliveryError::FailedClosed)
    ));
    assert!(inbox.is_failed_closed());
    let recovered = new_inbox(journal)?;
    assert!(recovered.is_failed_closed());
    assert!(matches!(
        recovered.actor_turn("command:request-32", 220),
        Err(ControlDeliveryError::FailedClosed)
    ));
    Ok(())
}

#[test]
fn crash_recovery_reemits_outbox_and_expired_leases_are_fenced()
-> Result<(), Box<dyn std::error::Error>> {
    let journal = MemoryJournal::default();
    let mut first = new_inbox(journal.clone())?;
    let install = claim(1, 100, 200, AccountDeliveryPurpose::Install)?;
    let ack = match first.accept_claim(install, 110)? {
        ClaimAcceptance::Install(output) => output.value().clone(),
        ClaimAcceptance::Reconcile(_) => return Err("install claim changed purpose".into()),
    };
    drop(first);

    let mut recovered = new_inbox(journal.clone())?;
    assert_eq!(recovered.pending_acknowledgements(150), vec![ack.clone()]);
    assert!(recovered.pending_acknowledgements(200).is_empty());
    assert!(recovered.actor_turn("command:request-32", 150)?.is_none());

    let renewed_install = claim(2, 200, 300, AccountDeliveryPurpose::Install)?;
    let renewed_ack = match recovered.accept_claim(renewed_install, 210)? {
        ClaimAcceptance::Install(output) => output.value().clone(),
        ClaimAcceptance::Reconcile(_) => return Err("unacked lease was reconciled".into()),
    };
    recovered.confirm_acknowledgement(&renewed_ack, 220)?;
    assert!(matches!(
        recovered.actor_turn("command:request-32", 300),
        Err(ControlDeliveryError::LeaseExpired)
    ));

    let reconciliation = claim(3, 300, 400, AccountDeliveryPurpose::ReconcileOnly)?;
    match recovered.accept_claim(reconciliation, 310)? {
        ClaimAcceptance::Reconcile(_) => {}
        ClaimAcceptance::Install(_) => return Err("expired ACK was reinstalled".into()),
    }
    drop(recovered);
    let recovered = new_inbox(journal)?;
    assert!(
        recovered
            .reconciliation_turn("command:request-32", 320)?
            .is_some()
    );
    Ok(())
}

#[test]
fn conflicting_duplicate_claim_is_persistently_fail_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let journal = MemoryJournal::default();
    let mut inbox = new_inbox(journal.clone())?;
    let accepted = claim(1, 100, 200, AccountDeliveryPurpose::Install)?;
    inbox.accept_claim(accepted, 110)?;

    let mut conflicting = claim(1, 100, 200, AccountDeliveryPurpose::Install)?;
    if let AccountDeliveryPayload::ControlCommand(command) = &mut conflicting.payload {
        command.action = ControlAction::Resume;
    }
    assert!(matches!(
        inbox.accept_claim(conflicting, 120),
        Err(ControlDeliveryError::FailedClosed)
    ));
    assert!(new_inbox(journal)?.is_failed_closed());
    Ok(())
}

#[test]
fn first_claim_and_scope_are_exactly_fenced() -> Result<(), Box<dyn std::error::Error>> {
    let mut skipped = new_inbox(MemoryJournal::default())?;
    assert!(matches!(
        skipped.accept_claim(claim(2, 100, 200, AccountDeliveryPurpose::Install)?, 110),
        Err(ControlDeliveryError::FailedClosed)
    ));

    let mut wrong_scope = new_inbox(MemoryJournal::default())?;
    let mut claim = claim(1, 100, 200, AccountDeliveryPurpose::Install)?;
    claim.lease.binding.config_epoch = 33;
    if let AccountDeliveryPayload::ControlCommand(command) = &mut claim.payload {
        command.expected_config_epoch = 33;
    }
    assert!(claim.validate().is_ok());
    assert!(matches!(
        wrong_scope.accept_claim(claim, 110),
        Err(ControlDeliveryError::FailedClosed)
    ));
    Ok(())
}

#[test]
fn opaque_storage_recovers_incomplete_tail_and_fences_a_stale_writer()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("control-delivery.jsonl");
    let mut first = ControlDeliveryInbox::recover(
        OpaqueControlDeliveryJournal::open(&path)?,
        binding()?,
        NODE,
    )?;
    let mut stale = ControlDeliveryInbox::recover(
        OpaqueControlDeliveryJournal::open(&path)?,
        binding()?,
        NODE,
    )?;
    first.accept_claim(claim(1, 100, 200, AccountDeliveryPurpose::Install)?, 110)?;
    assert!(matches!(
        stale.accept_claim(claim(1, 100, 200, AccountDeliveryPurpose::Install)?, 110),
        Err(ControlDeliveryError::Journal(
            ControlDeliveryJournalError::SequenceConflict
        ))
    ));
    drop(first);
    drop(stale);

    let mut file = OpenOptions::new().append(true).open(&path)?;
    file.write_all(b"{\"incomplete\"")?;
    file.sync_data()?;
    drop(file);

    let recovered = ControlDeliveryInbox::recover(
        OpaqueControlDeliveryJournal::open(&path)?,
        binding()?,
        NODE,
    )?;
    assert_eq!(recovered.pending_acknowledgements(150).len(), 1);
    assert!(std::fs::read(&path)?.ends_with(b"\n"));
    Ok(())
}

#[test]
fn control_http_client_requires_exact_loopback_and_bounded_timeouts() {
    for rejected in [
        "http://localhost:8080/",
        "https://127.0.0.1:8080/",
        "http://127.0.0.1/",
        "http://127.0.0.1:8080/control",
        "http://127.0.0.1:8080/?token=secret",
    ] {
        assert!(matches!(
            ControlHttpClient::new(ControlHttpClientConfig::local(rejected)),
            Err(ControlHttpClientError::InvalidConfig)
        ));
    }
    let mut timeout = ControlHttpClientConfig::local("http://127.0.0.1:8080/");
    timeout.request_timeout = Duration::from_secs(11);
    assert!(matches!(
        ControlHttpClient::new(timeout),
        Err(ControlHttpClientError::InvalidConfig)
    ));
}

#[tokio::test]
async fn polling_driver_orders_claim_durable_ack_actor_and_receipt()
-> Result<(), Box<dyn std::error::Error>> {
    let install = claim(1, 100, 1_100, AccountDeliveryPurpose::Install)?;
    let claim_body = serde_json::to_vec(&vec![install])?;
    let (base_url, server) = spawn_control_server(claim_body, 3).await?;
    let client = ControlHttpClient::new(ControlHttpClientConfig::local(base_url))?;
    let journal = MemoryJournal::default();
    let inbox = new_inbox(journal.clone())?;
    let mut driver = ControlDeliveryDriver::new(client, inbox, 1_000, 1)?;

    let mut work = driver.poll(110).await?;
    assert_eq!(journal.len()?, 3);
    assert_eq!(work.len(), 1);
    let actor = match work.pop() {
        Some(ControlDeliveryWork::Actor(turn)) => turn,
        Some(ControlDeliveryWork::Reconcile(_)) => {
            return Err("install claim became reconciliation".into());
        }
        None => return Err("actor work missing".into()),
    };
    assert!(!driver.grants_gateway_capability());
    assert!(!driver.grants_writer_lease());
    assert!(!driver.grants_wal_authority());
    assert!(!driver.grants_dispatch_permit());
    driver
        .submit_actor_completion(actor.applied(130, digest(4), "actor durable")?, 140)
        .await?;
    assert_eq!(journal.len()?, 5);

    let paths = server.await??;
    assert_eq!(
        paths,
        vec![
            "/v2/account-node/deliveries/claim",
            "/v2/account-node/deliveries/ack",
            "/v2/account-node/deliveries/receipts"
        ]
    );
    Ok(())
}

async fn spawn_control_server(
    claim_body: Vec<u8>,
    requests: usize,
) -> Result<
    (
        String,
        tokio::task::JoinHandle<Result<Vec<String>, std::io::Error>>,
    ),
    std::io::Error,
> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        let mut paths = Vec::new();
        for index in 0..requests {
            let (mut stream, _) = listener.accept().await?;
            let (path, request_body) = read_http_request(&mut stream).await?;
            let response_body = if index == 0 {
                claim_body.clone()
            } else {
                request_body
            };
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            );
            stream.write_all(headers.as_bytes()).await?;
            stream.write_all(&response_body).await?;
            stream.shutdown().await?;
            paths.push(path);
        }
        Ok(paths)
    });
    Ok((format!("http://{address}/"), server))
}

async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
) -> Result<(String, Vec<u8>), std::io::Error> {
    const MAX_TEST_REQUEST: usize = 128 * 1024;
    let mut encoded = Vec::new();
    let header_end = loop {
        if let Some(index) = encoded.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if encoded.len() >= MAX_TEST_REQUEST {
            return Err(std::io::Error::other("test request exceeds bound"));
        }
        let mut chunk = [0_u8; 4_096];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(std::io::Error::other("test request ended before headers"));
        }
        encoded.extend_from_slice(&chunk[..read]);
    };
    let headers = std::str::from_utf8(&encoded[..header_end])
        .map_err(|_| std::io::Error::other("test request headers are not UTF-8"))?;
    let mut lines = headers.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| std::io::Error::other("test request line missing"))?;
    let path = request_line
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| std::io::Error::other("test request path missing"))?
        .to_owned();
    let content_length = lines
        .find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
        })
        .ok_or_else(|| std::io::Error::other("test content length missing"))?;
    let total = header_end
        .checked_add(content_length)
        .ok_or_else(|| std::io::Error::other("test request length overflow"))?;
    if total > MAX_TEST_REQUEST {
        return Err(std::io::Error::other("test request exceeds bound"));
    }
    while encoded.len() < total {
        let remaining = total - encoded.len();
        let mut chunk = vec![0_u8; remaining.min(4_096)];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(std::io::Error::other("test request body ended early"));
        }
        encoded.extend_from_slice(&chunk[..read]);
    }
    Ok((path, encoded[header_end..total].to_vec()))
}

fn new_inbox(
    journal: MemoryJournal,
) -> Result<ControlDeliveryInbox<MemoryJournal>, Box<dyn std::error::Error>> {
    Ok(ControlDeliveryInbox::recover(journal, binding()?, NODE)?)
}

fn install_ack(
    inbox: &mut ControlDeliveryInbox<MemoryJournal>,
    claim: AccountDeliveryClaim,
    received_ms: u64,
    confirmed_ms: u64,
) -> Result<venue_control_protocol::AccountDeliveryAck, Box<dyn std::error::Error>> {
    let ack = match inbox.accept_claim(claim, received_ms)? {
        ClaimAcceptance::Install(output) => output.value().clone(),
        ClaimAcceptance::Reconcile(_) => return Err("install claim changed purpose".into()),
    };
    inbox.confirm_acknowledgement(&ack, confirmed_ms)?;
    Ok(ack)
}

fn binding() -> Result<AccountDeliveryBinding, venue_domain::SymbolError> {
    Ok(AccountDeliveryBinding {
        venue: VenueId::Binance,
        mode: GatewayMode::Test,
        trading_account_id: ACCOUNT.to_owned(),
        symbol: Symbol::new("BTC", "USDT")?,
        instance_id: "grid-btc".to_owned(),
        config_epoch: 32,
    })
}

fn claim(
    lease_epoch: u64,
    leased_at_ms: u64,
    expires_at_ms: u64,
    purpose: AccountDeliveryPurpose,
) -> Result<AccountDeliveryClaim, venue_domain::SymbolError> {
    let binding = binding()?;
    Ok(AccountDeliveryClaim {
        lease: AccountDeliveryLease {
            schema_version: ACCOUNT_DELIVERY_SCHEMA_VERSION,
            delivery_id: "command:request-32".to_owned(),
            binding: binding.clone(),
            node_id: NODE.to_owned(),
            lease_epoch,
            leased_at_ms,
            expires_at_ms,
            purpose,
        },
        payload: AccountDeliveryPayload::ControlCommand(ControlCommandRequest {
            schema_version: CONTROL_SCHEMA_VERSION,
            request_id: "request-32".to_owned(),
            venue: binding.venue,
            mode: binding.mode,
            trading_account_id: binding.trading_account_id,
            instance_id: binding.instance_id,
            symbol: binding.symbol,
            action: ControlAction::Pause,
            expected_config_epoch: binding.config_epoch,
            confirmation: None,
        }),
    })
}

fn digest(value: u8) -> [u8; 32] {
    [value; 32]
}
