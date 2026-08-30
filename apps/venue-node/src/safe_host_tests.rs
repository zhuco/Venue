use std::{
    collections::{BTreeSet, VecDeque},
    ffi::OsString,
    fs::OpenOptions,
    future::Future,
    io::Write,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread::{Thread, ThreadId},
    time::{Duration, Instant},
};

use rust_decimal::Decimal;
use tempfile::TempDir;
use venue_control_protocol::{CONTROL_SCHEMA_VERSION, ControlCommandRequest};
use venue_domain::domain::{
    CommandId, ExecutionCommand, OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide,
    Price,
};
use venue_execution::{CommandJournal, CommandState, WriterLeaseError};
use venue_gateway_api::{
    CanaryAdmissionReceipt, CapabilityFlags, CapabilityProbeCandidate, CapabilitySnapshot,
    CompleteOrderFamilyEvidence, ControlAppliedReceipt, ControlState, EvidenceCommitment,
    GatewayBinding, GatewayMode, HostAdmissionEvidence, HostAdmittedCapability,
    OrderFamilyEvidence, OrderFamilySupport, OwnerRecoveryReceipt, PromotionScope, VenueId,
    WalRecoveryReceipt, WriterFenceReceipt, promote_capability,
};
use venue_runtime::{AccountKey, StrategyBinding, StrategyInstanceKey, StrategyKind};

use crate::{
    AsyncGatewayBoundaryError, AsyncGatewayCallError, AsyncGatewayTimeouts, AsyncPhysicalGateway,
    CanaryControlRequest, CanaryEvidence, ControlAction, DispatchOutcome, FamilyReadbackCoverage,
    GatewayAcknowledgement, GatewayDispatchResult, GatewayRecoveryPermit, NodeLaunch,
    NodeSafetyHost, PhysicalGateway, ReadbackCommandState, SafeHostError, SignedCommandReadback,
    SignedReadbackReceipt, SignedReadbackRequest, TokioPhysicalGateway, TokioRuntimeDriver,
    TokioRuntimeRun, safe_host::TestCrashPoint,
};

const ACCOUNT: &str = "00000000-0000-4000-8000-000000000010";
const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CONFIG_DIGEST: &str = "cfg_1";

#[derive(Clone)]
enum ReadbackResolution {
    Accepted(&'static str),
    Rejected(&'static str),
    Absent,
}

#[derive(Clone)]
struct ReadbackPlan {
    connection_generation: u64,
    private_generation: u64,
    observed_ms: u64,
    resolution: ReadbackResolution,
    nonzero_position: bool,
    omit_family: bool,
}

impl ReadbackPlan {
    fn initial() -> Self {
        Self {
            connection_generation: 1,
            private_generation: 1,
            observed_ms: 900,
            resolution: ReadbackResolution::Absent,
            nonzero_position: false,
            omit_family: false,
        }
    }

    fn recovery(resolution: ReadbackResolution) -> Self {
        Self {
            connection_generation: 2,
            private_generation: 2,
            observed_ms: 11_900,
            resolution,
            nonzero_position: false,
            omit_family: false,
        }
    }
}

struct FakeGateway {
    binding: GatewayBinding,
    capability: CapabilitySnapshot,
    readbacks: VecDeque<ReadbackPlan>,
    dispatches: VecDeque<GatewayDispatchResult>,
    dispatch_calls: Arc<AtomicUsize>,
    connect_calls: Arc<AtomicUsize>,
    unsupported_regular: bool,
    connected: bool,
}

impl FakeGateway {
    fn new(binding: GatewayBinding, readbacks: Vec<ReadbackPlan>) -> Self {
        let capability = CapabilitySnapshot {
            binding: binding.clone(),
            version: 7,
            observed_ms: 500,
            expires_ms: 100_000,
            flags: CapabilityFlags::READ_ACCOUNT
                | CapabilityFlags::READ_ORDERS
                | CapabilityFlags::READ_FILLS
                | CapabilityFlags::PRIVATE_STREAM
                | CapabilityFlags::TRADE
                | CapabilityFlags::PLACE_LIMIT
                | CapabilityFlags::PLACE_MARKET
                | CapabilityFlags::CANCEL,
        };
        Self {
            binding,
            capability,
            readbacks: readbacks.into(),
            dispatches: VecDeque::new(),
            dispatch_calls: Arc::new(AtomicUsize::new(0)),
            connect_calls: Arc::new(AtomicUsize::new(0)),
            unsupported_regular: false,
            connected: false,
        }
    }

    fn with_dispatches(mut self, dispatches: Vec<GatewayDispatchResult>) -> Self {
        self.dispatches = dispatches.into();
        self
    }

    fn with_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.dispatch_calls = counter;
        self
    }

    fn with_connect_counter(mut self, counter: Arc<AtomicUsize>) -> Self {
        self.connect_calls = counter;
        self
    }

    fn with_unsupported_regular(mut self) -> Self {
        self.unsupported_regular = true;
        self
    }

    fn with_empty_capability(mut self) -> Self {
        self.capability.flags = CapabilityFlags::empty();
        self
    }

    fn with_read_only_capability(mut self) -> Self {
        self.capability.flags = CapabilityFlags::READ_ACCOUNT
            | CapabilityFlags::READ_ORDERS
            | CapabilityFlags::READ_FILLS
            | CapabilityFlags::PRIVATE_STREAM;
        self
    }
}

impl PhysicalGateway for FakeGateway {
    type Error = ();

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.capability.clone()
    }

    fn connect_after_recovery(&mut self, permit: GatewayRecoveryPermit) -> Result<(), Self::Error> {
        if permit.binding() != &self.binding || permit.config_epoch() != 1 || self.connected {
            return Err(());
        }
        self.connect_calls.fetch_add(1, Ordering::SeqCst);
        self.connected = true;
        Ok(())
    }

    fn signed_readback(
        &mut self,
        request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        if !self.connected {
            return Err(());
        }
        let plan = self.readbacks.pop_front().ok_or(())?;
        receipt_from_plan(request, plan, self.unsupported_regular)
    }

    fn verify_signed_readback(&self, receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        (receipt.commitment_sha256() == DIGEST)
            .then_some(())
            .ok_or(())
    }

    fn dispatch(&mut self, permit: crate::DispatchPermit) -> GatewayDispatchResult {
        assert_eq!(permit.binding(), &self.binding);
        self.dispatch_calls.fetch_add(1, Ordering::SeqCst);
        self.dispatches
            .pop_front()
            .unwrap_or(GatewayDispatchResult::Rejected {
                reason_code: "fake_missing_result".to_owned(),
            })
    }
}

fn receipt_from_plan(
    request: &SignedReadbackRequest,
    plan: ReadbackPlan,
    unsupported_regular: bool,
) -> Result<SignedReadbackReceipt, ()> {
    let regular = if unsupported_regular {
        FamilyReadbackCoverage::unsupported(venue_domain::domain::NativeOrderFamily::UmOrder)
    } else {
        FamilyReadbackCoverage::complete(venue_domain::domain::NativeOrderFamily::UmOrder)
    };
    let mut coverage = vec![
        regular,
        FamilyReadbackCoverage::complete(venue_domain::domain::NativeOrderFamily::UmConditional),
        FamilyReadbackCoverage::complete(venue_domain::domain::NativeOrderFamily::UmAlgo),
    ];
    if plan.omit_family {
        let _ = coverage.pop();
    }
    let command_results = request
        .commands()
        .iter()
        .cloned()
        .map(|key| {
            let state = match plan.resolution {
                ReadbackResolution::Accepted(venue_order_id) => ReadbackCommandState::Accepted {
                    venue_order_id: venue_order_id.to_owned(),
                },
                ReadbackResolution::Rejected(reason_code) => ReadbackCommandState::Rejected {
                    reason_code: reason_code.to_owned(),
                },
                ReadbackResolution::Absent => ReadbackCommandState::ProvenAbsent,
            };
            SignedCommandReadback::new(key, state).map_err(|_| ())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let nonzero_position_symbols = if plan.nonzero_position {
        BTreeSet::from([request.binding().symbol.clone()])
    } else {
        BTreeSet::new()
    };
    SignedReadbackReceipt::new(
        request.binding().clone(),
        plan.connection_generation,
        plan.private_generation,
        plan.observed_ms,
        DIGEST,
        coverage,
        Vec::new(),
        nonzero_position_symbols,
        command_results,
    )
    .map_err(|_| ())
}

#[derive(Clone, Copy)]
enum AsyncDispatchPlan {
    Acknowledged,
    Timeout,
    Disconnected,
}

struct FakeAsyncGateway {
    binding: GatewayBinding,
    capability: CapabilitySnapshot,
    readbacks: VecDeque<ReadbackPlan>,
    dispatches: VecDeque<AsyncDispatchPlan>,
    connect_calls: Arc<AtomicUsize>,
    readback_calls: Arc<AtomicUsize>,
    dispatch_calls: Arc<AtomicUsize>,
    runtime_threads: Arc<Mutex<Vec<ThreadId>>>,
    connected: bool,
}

impl FakeAsyncGateway {
    fn new(
        binding: GatewayBinding,
        readbacks: Vec<ReadbackPlan>,
        dispatches: Vec<AsyncDispatchPlan>,
        counters: AsyncGatewayCounters,
    ) -> Self {
        let capability = FakeGateway::new(binding.clone(), Vec::new()).capability;
        Self {
            binding,
            capability,
            readbacks: readbacks.into(),
            dispatches: dispatches.into(),
            connect_calls: counters.connect,
            readback_calls: counters.readback,
            dispatch_calls: counters.dispatch,
            runtime_threads: counters.runtime_threads,
            connected: false,
        }
    }
}

#[derive(Clone)]
struct AsyncGatewayCounters {
    connect: Arc<AtomicUsize>,
    readback: Arc<AtomicUsize>,
    dispatch: Arc<AtomicUsize>,
    runtime_threads: Arc<Mutex<Vec<ThreadId>>>,
}

impl AsyncGatewayCounters {
    fn new() -> Self {
        Self {
            connect: Arc::new(AtomicUsize::new(0)),
            readback: Arc::new(AtomicUsize::new(0)),
            dispatch: Arc::new(AtomicUsize::new(0)),
            runtime_threads: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

struct TestTokioRuntime {
    execution_now_ms: u64,
    completed_runs: usize,
    advance_after_run: Option<(usize, u64)>,
}

impl TestTokioRuntime {
    const fn at(execution_now_ms: u64) -> Self {
        Self {
            execution_now_ms,
            completed_runs: 0,
            advance_after_run: None,
        }
    }

    const fn advance_after_run(
        execution_now_ms: u64,
        run_number: usize,
        advanced_now_ms: u64,
    ) -> Self {
        Self {
            execution_now_ms,
            completed_runs: 0,
            advance_after_run: Some((run_number, advanced_now_ms)),
        }
    }

    fn complete_run(&mut self) {
        self.completed_runs = self.completed_runs.saturating_add(1);
        if self
            .advance_after_run
            .is_some_and(|(run_number, _)| run_number == self.completed_runs)
            && let Some((_, advanced_now_ms)) = self.advance_after_run.take()
        {
            self.execution_now_ms = advanced_now_ms;
        }
    }
}

struct ThreadWake {
    thread: Thread,
}

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.thread.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.thread.unpark();
    }
}

impl TokioRuntimeDriver for TestTokioRuntime {
    fn run<F: Future + Send>(
        &mut self,
        timeout: Duration,
        future: F,
    ) -> TokioRuntimeRun<F::Output> {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return TokioRuntimeRun::Failed;
        };
        let waker = Waker::from(Arc::new(ThreadWake {
            thread: std::thread::current(),
        }));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => {
                    self.complete_run();
                    return TokioRuntimeRun::Completed(output);
                }
                Poll::Pending => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        self.complete_run();
                        return TokioRuntimeRun::TimedOut;
                    }
                    std::thread::park_timeout(remaining);
                }
            }
        }
    }

    fn execution_now_ms(&self) -> u64 {
        self.execution_now_ms
    }
}

fn record_runtime_thread(threads: &Mutex<Vec<ThreadId>>) -> Result<(), ()> {
    threads
        .lock()
        .map_err(|_| ())?
        .push(std::thread::current().id());
    Ok(())
}

impl AsyncPhysicalGateway for FakeAsyncGateway {
    type Error = ();

    fn binding(&self) -> &GatewayBinding {
        &self.binding
    }

    fn capability_snapshot(&self) -> CapabilitySnapshot {
        self.capability.clone()
    }

    fn connect_after_recovery(
        &mut self,
        permit: GatewayRecoveryPermit,
    ) -> impl Future<Output = Result<(), AsyncGatewayCallError<Self::Error>>> + Send {
        let result = if permit.binding() == &self.binding && !self.connected {
            self.connected = true;
            Ok(())
        } else {
            Err(AsyncGatewayCallError::Failed(()))
        };
        let calls = Arc::clone(&self.connect_calls);
        let threads = Arc::clone(&self.runtime_threads);
        async move {
            record_runtime_thread(&threads).map_err(AsyncGatewayCallError::Failed)?;
            calls.fetch_add(1, Ordering::SeqCst);
            result
        }
    }

    fn signed_readback(
        &mut self,
        request: SignedReadbackRequest,
    ) -> impl Future<Output = Result<SignedReadbackReceipt, AsyncGatewayCallError<Self::Error>>> + Send
    {
        let result = if self.connected {
            self.readbacks
                .pop_front()
                .ok_or(())
                .and_then(|plan| receipt_from_plan(&request, plan, false))
                .map_err(AsyncGatewayCallError::Failed)
        } else {
            Err(AsyncGatewayCallError::Disconnected)
        };
        let calls = Arc::clone(&self.readback_calls);
        let threads = Arc::clone(&self.runtime_threads);
        async move {
            record_runtime_thread(&threads).map_err(AsyncGatewayCallError::Failed)?;
            calls.fetch_add(1, Ordering::SeqCst);
            result
        }
    }

    fn verify_signed_readback(&self, receipt: &SignedReadbackReceipt) -> Result<(), Self::Error> {
        (receipt.commitment_sha256() == DIGEST)
            .then_some(())
            .ok_or(())
    }

    fn dispatch(
        &mut self,
        admitted_capability: HostAdmittedCapability,
        admission_evidence: HostAdmissionEvidence,
        permit: crate::DispatchPermit,
    ) -> impl Future<Output = Result<GatewayDispatchResult, AsyncGatewayCallError<Self::Error>>> + Send
    {
        let binding_matches = permit.binding() == &self.binding
            && admitted_capability.scope().binding() == &self.binding
            && admitted_capability.scope() == admission_evidence.scope();
        let plan = self.dispatches.pop_front();
        let calls = Arc::clone(&self.dispatch_calls);
        let threads = Arc::clone(&self.runtime_threads);
        async move {
            record_runtime_thread(&threads).map_err(AsyncGatewayCallError::Failed)?;
            calls.fetch_add(1, Ordering::SeqCst);
            if !binding_matches {
                return Err(AsyncGatewayCallError::Failed(()));
            }
            match plan {
                Some(AsyncDispatchPlan::Acknowledged) => Ok(GatewayDispatchResult::Acknowledged(
                    GatewayAcknowledgement::new("venue_async_ack")
                        .map_err(|_| AsyncGatewayCallError::Failed(()))?,
                )),
                Some(AsyncDispatchPlan::Timeout) => std::future::pending().await,
                Some(AsyncDispatchPlan::Disconnected) => Err(AsyncGatewayCallError::Disconnected),
                None => Err(AsyncGatewayCallError::Failed(())),
            }
        }
    }
}

fn launch(directory: &TempDir, mode: &str) -> Result<NodeLaunch, Box<dyn std::error::Error>> {
    let arguments = vec![
        OsString::from("venue-node-bybit"),
        OsString::from("--mode"),
        OsString::from(mode),
        OsString::from("--trading-account-id"),
        OsString::from(ACCOUNT),
        OsString::from("--symbol"),
        OsString::from("BTC/USDT"),
        OsString::from("--artifacts-base"),
        directory.path().as_os_str().to_owned(),
    ];
    Ok(NodeLaunch::try_parse_from(VenueId::Bybit, arguments)?)
}

fn owner(binding: &GatewayBinding) -> Result<StrategyBinding, Box<dyn std::error::Error>> {
    let account = AccountKey::new(binding.venue, binding.trading_account_id.clone())?;
    let key = StrategyInstanceKey::new(
        account,
        StrategyKind::HedgedGrid,
        "grid_btc_primary",
        binding.symbol.clone(),
    )?;
    Ok(StrategyBinding::new(key, "run_1", CONFIG_DIGEST)?)
}

fn canary(
    binding: &GatewayBinding,
    owner: &StrategyBinding,
) -> Result<CanaryEvidence, Box<dyn std::error::Error>> {
    canary_for(binding, owner, "unused_canary_command")
}

fn canary_for(
    binding: &GatewayBinding,
    owner: &StrategyBinding,
    command_id: &str,
) -> Result<CanaryEvidence, Box<dyn std::error::Error>> {
    Ok(CanaryEvidence::new(
        binding.clone(),
        owner,
        7,
        1,
        800,
        100_000,
        CommandId::new(command_id)?,
        DIGEST,
    )?)
}

fn commitment(byte: char) -> Result<EvidenceCommitment, Box<dyn std::error::Error>> {
    Ok(EvidenceCommitment::new(byte.to_string().repeat(64))?)
}

fn admitted_capability(
    binding: &GatewayBinding,
    prepared: &crate::PreparedDispatch,
    now_ms: u64,
) -> Result<(HostAdmittedCapability, HostAdmissionEvidence), Box<dyn std::error::Error>> {
    let scope = PromotionScope::new(
        binding.clone(),
        prepared.config_epoch(),
        prepared.connection_generation(),
        prepared.private_generation(),
    )?;
    let order_families = CompleteOrderFamilyEvidence::new(
        OrderFamilyEvidence::new(OrderFamilySupport::Complete, commitment('1')?),
        OrderFamilyEvidence::new(OrderFamilySupport::Complete, commitment('2')?),
        OrderFamilyEvidence::new(OrderFamilySupport::Complete, commitment('3')?),
    );
    let expires_ms = now_ms.checked_add(10_000).ok_or("test expiry overflow")?;
    let candidate = CapabilityProbeCandidate::from_snapshot(
        CapabilitySnapshot {
            binding: binding.clone(),
            version: 7,
            observed_ms: now_ms,
            expires_ms,
            flags: CapabilityFlags::READ_ACCOUNT
                | CapabilityFlags::READ_ORDERS
                | CapabilityFlags::READ_FILLS
                | CapabilityFlags::PRIVATE_STREAM
                | CapabilityFlags::TRADE
                | CapabilityFlags::PLACE_LIMIT
                | CapabilityFlags::PLACE_MARKET
                | CapabilityFlags::CANCEL,
        },
        prepared.connection_generation(),
        prepared.private_generation(),
        order_families.clone(),
        commitment('4')?,
    )?;
    let evidence = HostAdmissionEvidence::new(
        scope.clone(),
        expires_ms,
        order_families,
        ControlAppliedReceipt::new(
            scope.clone(),
            ControlState::Active,
            1,
            now_ms,
            commitment('5')?,
        )?,
        OwnerRecoveryReceipt::new(scope.clone(), 1, now_ms, commitment('6')?)?,
        WalRecoveryReceipt::new(scope.clone(), 1, 0, 0, now_ms, commitment('7')?)?,
        WriterFenceReceipt::new(
            scope.clone(),
            prepared.writer_generation(),
            prepared.writer_revision(),
            now_ms,
            commitment('8')?,
        )?,
        CanaryAdmissionReceipt::new(scope, 1, 7, now_ms, expires_ms, commitment('9')?)?,
    )?;
    let capability = promote_capability(&candidate, evidence.clone(), now_ms)?;
    Ok((capability, evidence))
}

fn entry_command(
    binding: &GatewayBinding,
    command_id: &str,
) -> Result<ExecutionCommand, Box<dyn std::error::Error>> {
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: CommandId::new(command_id)?,
        client_order_id: CommandId::new(format!("client_{command_id}"))?,
        owner: OrderOwner {
            strategy_instance_id: "grid_btc_primary".to_owned(),
            run_id: "run_1".to_owned(),
            exchange: binding.venue.as_str().to_owned(),
            account: binding.trading_account_id.clone(),
            symbol: binding.symbol.clone(),
            purpose: OrderPurpose::Entry,
        },
        side: OrderSide::Buy,
        position_side: PositionSide::Long,
        quantity: Decimal::ONE,
        limit_price: Price::new(Decimal::ONE)?,
        reduce_only: false,
    }))
}

fn journal_path(launch: &NodeLaunch) -> PathBuf {
    launch
        .artifacts_root()
        .join("account")
        .join("commands.jsonl")
}

fn supervision_path(launch: &NodeLaunch) -> PathBuf {
    launch
        .artifacts_root()
        .join("account")
        .join("control_receipts.jsonl")
}

fn control_request(
    binding: &GatewayBinding,
    action: ControlAction,
    request_id: &str,
) -> ControlCommandRequest {
    let mut request = ControlCommandRequest {
        schema_version: CONTROL_SCHEMA_VERSION,
        request_id: request_id.to_owned(),
        venue: binding.venue,
        mode: binding.mode,
        trading_account_id: binding.trading_account_id.clone(),
        instance_id: "grid_btc_primary".to_owned(),
        symbol: binding.symbol.clone(),
        action,
        expected_config_epoch: 1,
        confirmation: None,
    };
    if action.requires_confirmation() {
        request.confirmation = Some(request.expected_confirmation());
    }
    request
}

fn apply_control<G: PhysicalGateway>(
    host: &mut NodeSafetyHost<G>,
    binding: &GatewayBinding,
    action: ControlAction,
    request_id: &str,
    now_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let turn = host.accept_control_command(control_request(binding, action, request_id), now_ms)?;
    let receipt = turn.persisted(1, DIGEST, now_ms)?;
    let _ = host.apply_control_receipt(receipt)?;
    Ok(())
}

#[test]
fn wrong_binding_or_mode_fails_before_artifact_creation() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut wrong_binding = launch.binding().clone();
    wrong_binding.mode = GatewayMode::Test;
    let gateway = FakeGateway::new(wrong_binding, vec![ReadbackPlan::initial()]);

    let result = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        gateway,
        Some(canary(launch.binding(), &owner)?),
        1_000,
    );

    assert!(matches!(result, Err(SafeHostError::BindingScope)));
    assert!(!launch.artifacts_root().exists());
    Ok(())
}

#[test]
fn fresh_writer_lease_rejects_a_second_host() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]),
        Some(canary(launch.binding(), &owner)?),
        1_000,
    )?;

    let second = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan {
                connection_generation: 1,
                private_generation: 2,
                observed_ms: 1_000,
                resolution: ReadbackResolution::Absent,
                nonzero_position: false,
                omit_family: false,
            }],
        ),
        Some(canary(launch.binding(), &owner)?),
        1_001,
    );

    assert!(matches!(
        second,
        Err(SafeHostError::Writer(WriterLeaseError::Fenced))
    ));
    drop(first);
    Ok(())
}

#[test]
fn prepared_crash_is_proven_not_dispatched_on_reopen() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let command = entry_command(launch.binding(), "command_prepared")?;
    let command_id = command.command_id().clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let mut first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
            .with_counter(Arc::clone(&counter)),
        Some(canary_for(launch.binding(), &owner, "command_prepared")?),
        1_000,
    )?;
    let _prepared = first.prepare_dispatch(command, 1_000)?;
    drop(first);

    let reopened = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::recovery(ReadbackResolution::Absent)],
        )
        .with_counter(Arc::clone(&counter)),
        Some(canary(launch.binding(), &owner)?),
        12_000,
    )?;
    let journal = CommandJournal::open(journal_path(&launch))?;

    assert!(matches!(
        journal.receipt(&command_id).map(|receipt| &receipt.state),
        Some(CommandState::Rejected { .. })
    ));
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    drop(reopened);
    Ok(())
}

#[test]
fn submitted_crash_becomes_unknown_and_uses_readback_without_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let command = entry_command(launch.binding(), "command_submitted")?;
    let command_id = command.command_id().clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let mut first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
            .with_counter(Arc::clone(&counter)),
        Some(canary_for(launch.binding(), &owner, "command_submitted")?),
        1_000,
    )?;
    let prepared = first.prepare_dispatch(command, 1_000)?;
    let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;
    assert!(matches!(
        first.dispatch_with_crash(
            prepared,
            capability,
            evidence,
            1_000,
            TestCrashPoint::AfterSubmitted
        ),
        Err(SafeHostError::InjectedCrash)
    ));
    drop(first);

    let reopened = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::recovery(ReadbackResolution::Absent)],
        )
        .with_counter(Arc::clone(&counter)),
        Some(canary(launch.binding(), &owner)?),
        12_000,
    )?;
    let journal = CommandJournal::open(journal_path(&launch))?;

    assert!(matches!(
        journal.receipt(&command_id).map(|receipt| &receipt.state),
        Some(CommandState::Rejected { .. })
    ));
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    drop(reopened);
    Ok(())
}

#[test]
fn crash_after_gateway_ack_recovers_accepted_without_resubmission()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let command = entry_command(launch.binding(), "command_after_ack")?;
    let command_id = command.command_id().clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let acknowledgement = GatewayAcknowledgement::new("venue_after_ack")?;
    let mut first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
            .with_dispatches(vec![GatewayDispatchResult::Acknowledged(acknowledgement)])
            .with_counter(Arc::clone(&counter)),
        Some(canary_for(launch.binding(), &owner, "command_after_ack")?),
        1_000,
    )?;
    let prepared = first.prepare_dispatch(command, 1_000)?;
    let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;
    assert!(matches!(
        first.dispatch_with_crash(
            prepared,
            capability,
            evidence,
            1_000,
            TestCrashPoint::AfterGatewayResult
        ),
        Err(SafeHostError::InjectedCrash)
    ));
    drop(first);

    let reopened = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::recovery(ReadbackResolution::Accepted(
                "venue_after_ack",
            ))],
        )
        .with_counter(Arc::clone(&counter)),
        Some(canary(launch.binding(), &owner)?),
        12_000,
    )?;
    let journal = CommandJournal::open(journal_path(&launch))?;

    assert!(matches!(
        journal.receipt(&command_id).map(|receipt| &receipt.state),
        Some(CommandState::Accepted { venue_order_id }) if venue_order_id == "venue_after_ack"
    ));
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    drop(reopened);
    Ok(())
}

#[test]
fn ack_then_disconnect_stays_unknown_until_signed_readback_and_never_retries()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let command = entry_command(launch.binding(), "command_disconnect")?;
    let counter = Arc::new(AtomicUsize::new(0));
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![
                ReadbackPlan::initial(),
                ReadbackPlan {
                    connection_generation: 2,
                    private_generation: 2,
                    observed_ms: 1_100,
                    resolution: ReadbackResolution::Accepted("venue_disconnect"),
                    nonzero_position: false,
                    omit_family: false,
                },
            ],
        )
        .with_dispatches(vec![GatewayDispatchResult::Unknown])
        .with_counter(Arc::clone(&counter)),
        Some(canary_for(launch.binding(), &owner, "command_disconnect")?),
        1_000,
    )?;
    let prepared = host.prepare_dispatch(command.clone(), 1_000)?;
    let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;

    assert_eq!(
        host.dispatch_admitted_for_test(prepared, capability, evidence, 1_000)?,
        DispatchOutcome::Unknown
    );
    host.recover_unknowns(1_200)?;
    assert!(matches!(
        host.prepare_dispatch(command, 1_200),
        Err(SafeHostError::CommandAlreadyJournaled)
    ));
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn async_timeout_and_disconnect_become_unknown_then_exact_readback_without_resubmit()
-> Result<(), Box<dyn std::error::Error>> {
    for (suffix, dispatch_plan) in [
        ("timeout", AsyncDispatchPlan::Timeout),
        ("disconnect", AsyncDispatchPlan::Disconnected),
    ] {
        let directory = tempfile::tempdir()?;
        let launch = launch(&directory, "LIVE")?;
        let owner = owner(launch.binding())?;
        let command_id = format!("async_{suffix}");
        let command = entry_command(launch.binding(), &command_id)?;
        let counters = AsyncGatewayCounters::new();
        let gateway = FakeAsyncGateway::new(
            launch.binding().clone(),
            vec![
                ReadbackPlan::initial(),
                ReadbackPlan {
                    connection_generation: 2,
                    private_generation: 2,
                    observed_ms: 1_100,
                    resolution: ReadbackResolution::Accepted("venue_async_exact"),
                    nonzero_position: false,
                    omit_family: false,
                },
            ],
            vec![dispatch_plan],
            counters.clone(),
        );
        let boundary = TokioPhysicalGateway::new(
            gateway,
            TestTokioRuntime::at(1_000),
            AsyncGatewayTimeouts::new(
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_millis(20),
            )?,
        )?;
        let mut host = NodeSafetyHost::open_for_test(
            &launch,
            owner.clone(),
            boundary,
            Some(canary_for(launch.binding(), &owner, &command_id)?),
            1_000,
        )?;
        let prepared = host.prepare_dispatch(command.clone(), 1_000)?;
        let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;

        assert_eq!(
            host.dispatch_admitted_for_test(prepared, capability, evidence, 1_000)?,
            DispatchOutcome::Unknown
        );
        assert_eq!(counters.dispatch.load(Ordering::SeqCst), 1);
        host.recover_unknowns(1_200)?;
        assert_eq!(counters.connect.load(Ordering::SeqCst), 1);
        assert_eq!(counters.readback.load(Ordering::SeqCst), 2);
        assert_eq!(counters.dispatch.load(Ordering::SeqCst), 1);
        assert!(matches!(
            host.prepare_dispatch(command, 1_200),
            Err(SafeHostError::CommandAlreadyJournaled)
        ));
        let journal = CommandJournal::open(journal_path(&launch))?;
        assert!(matches!(
            journal
                .receipt(&CommandId::new(command_id.clone())?)
                .map(|receipt| &receipt.state),
            Some(CommandState::Accepted { venue_order_id })
                if venue_order_id == "venue_async_exact"
        ));
        let threads = counters
            .runtime_threads
            .lock()
            .map_err(|_| std::io::Error::other("runtime thread record poisoned"))?;
        let Some(first_thread) = threads.first() else {
            return Err(std::io::Error::other("missing runtime thread record").into());
        };
        assert_eq!(threads.len(), 4);
        assert!(threads.iter().all(|thread| thread == first_thread));
    }
    Ok(())
}

#[test]
fn async_boundary_rechecks_ttl_at_actual_execution_clock() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let counters = AsyncGatewayCounters::new();
    let boundary = TokioPhysicalGateway::new(
        FakeAsyncGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::initial()],
            Vec::new(),
            counters.clone(),
        ),
        TestTokioRuntime::at(11_000),
        AsyncGatewayTimeouts::default(),
    )?;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        boundary,
        Some(canary_for(
            launch.binding(),
            &owner,
            "async_expired_at_send",
        )?),
        1_000,
    )?;
    let command_id = CommandId::new("async_expired_at_send")?;
    let prepared =
        host.prepare_dispatch(entry_command(launch.binding(), command_id.as_str())?, 1_000)?;
    let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;

    assert_eq!(
        host.dispatch_admitted_for_test(prepared, capability, evidence, 1_000)?,
        DispatchOutcome::Rejected {
            reason_code: "host_admission_invalid".to_owned(),
        }
    );
    assert_eq!(counters.dispatch.load(Ordering::SeqCst), 0);
    let journal = CommandJournal::open(journal_path(&launch))?;
    assert!(matches!(
        journal.receipt(&command_id).map(|receipt| &receipt.state),
        Some(CommandState::Rejected { reason }) if reason == "host_admission_invalid"
    ));
    Ok(())
}

#[test]
fn capability_binding_mismatch_fails_before_artifacts_or_gateway_calls()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut gateway = FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]);
    gateway.capability.binding.mode = GatewayMode::Test;
    let result = NodeSafetyHost::open_for_test(&launch, owner, gateway, None, 1_000);

    assert!(matches!(
        result,
        Err(SafeHostError::AsyncGateway(
            AsyncGatewayBoundaryError::CapabilityScope
        ))
    ));
    assert!(!launch.artifacts_root().exists());
    Ok(())
}

#[test]
fn empty_capability_fails_before_artifacts_or_gateway_calls()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let gateway = FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
        .with_connect_counter(Arc::clone(&connect_calls))
        .with_empty_capability();

    let result = NodeSafetyHost::open_for_test(&launch, owner, gateway, None, 1_000);

    assert!(matches!(
        result,
        Err(SafeHostError::AsyncGateway(
            AsyncGatewayBoundaryError::CapabilityClosed
        ))
    ));
    assert_eq!(connect_calls.load(Ordering::SeqCst), 0);
    assert!(!launch.artifacts_root().exists());
    Ok(())
}

#[test]
fn read_only_recovery_capability_connects_but_does_not_grant_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let dispatch_calls = Arc::new(AtomicUsize::new(0));
    let host = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
            .with_read_only_capability()
            .with_counter(Arc::clone(&dispatch_calls)),
        None,
        1_000,
    )?;

    assert_eq!(host.binding(), launch.binding());
    assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn publicly_constructed_token_cannot_dispatch_in_production_path()
-> Result<(), Box<dyn std::error::Error>> {
    for entry in ["dispatch", "admit"] {
        let directory = tempfile::tempdir()?;
        let launch = launch(&directory, "LIVE")?;
        let owner = owner(launch.binding())?;
        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let command_id = CommandId::new(format!("forged_public_{entry}"))?;
        let mut host = NodeSafetyHost::open_for_test(
            &launch,
            owner.clone(),
            FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
                .with_counter(Arc::clone(&dispatch_calls)),
            Some(canary_for(launch.binding(), &owner, command_id.as_str())?),
            1_000,
        )?;
        let prepared =
            host.prepare_dispatch(entry_command(launch.binding(), command_id.as_str())?, 1_000)?;
        let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;
        let unavailable = if entry == "dispatch" {
            host.dispatch_prepared(prepared, capability, evidence, 1_000)
                .map(|_| ())
        } else {
            host.admit_prepared(prepared, capability, evidence, 1_000)
        };

        assert!(matches!(
            unavailable,
            Err(SafeHostError::HostAdmissionUnavailable)
        ));
        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
        let journal = CommandJournal::open(journal_path(&launch))?;
        assert!(matches!(
            journal.receipt(&command_id).map(|receipt| &receipt.state),
            Some(CommandState::Rejected { reason }) if reason == "host_admission_unavailable"
        ));
    }
    Ok(())
}

#[test]
fn canary_expiry_after_prepare_is_durably_rejected_without_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let command_id = CommandId::new("canary_expires_after_prepare")?;
    let dispatch_calls = Arc::new(AtomicUsize::new(0));
    let expiring_canary = CanaryEvidence::new(
        launch.binding().clone(),
        &owner,
        7,
        1,
        800,
        1_001,
        command_id.clone(),
        DIGEST,
    )?;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
            .with_counter(Arc::clone(&dispatch_calls)),
        Some(expiring_canary),
        1_000,
    )?;
    let prepared =
        host.prepare_dispatch(entry_command(launch.binding(), command_id.as_str())?, 1_000)?;
    let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;

    assert!(matches!(
        host.dispatch_admitted_for_test(prepared, capability, evidence, 1_001),
        Err(SafeHostError::CanaryEvidence)
    ));
    assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
    let journal = CommandJournal::open(journal_path(&launch))?;
    assert!(matches!(
        journal.receipt(&command_id).map(|receipt| &receipt.state),
        Some(CommandState::Rejected { reason }) if reason == "dispatch_revalidation_failed"
    ));
    Ok(())
}

#[test]
fn mismatched_host_admission_fails_before_physical_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let dispatch_calls = Arc::new(AtomicUsize::new(0));
    let command = entry_command(launch.binding(), "stale_host_admission")?;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
            .with_counter(Arc::clone(&dispatch_calls)),
        Some(canary_for(
            launch.binding(),
            &owner,
            "stale_host_admission",
        )?),
        1_000,
    )?;
    let prepared = host.prepare_dispatch(command, 1_000)?;
    let (capability, _) = admitted_capability(launch.binding(), &prepared, 1_000)?;
    let (_, drifted_evidence) = admitted_capability(launch.binding(), &prepared, 1_001)?;

    assert!(matches!(
        host.dispatch_admitted_for_test(prepared, capability, drifted_evidence, 1_001),
        Err(SafeHostError::CapabilityPromotion(
            venue_gateway_api::CapabilityPromotionError::Scope
                | venue_gateway_api::CapabilityPromotionError::Drift
        ))
    ));
    assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
    let journal = CommandJournal::open(journal_path(&launch))?;
    assert!(matches!(
        journal
            .receipt(&CommandId::new("stale_host_admission")?)
            .map(|receipt| &receipt.state),
        Some(CommandState::Rejected { reason }) if reason == "dispatch_revalidation_failed"
    ));
    Ok(())
}

#[test]
fn async_completion_after_admission_ttl_is_unknown_and_never_retried()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let command_id = CommandId::new("async_expired_after_await")?;
    let counters = AsyncGatewayCounters::new();
    let boundary = TokioPhysicalGateway::new(
        FakeAsyncGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::initial()],
            vec![AsyncDispatchPlan::Acknowledged],
            counters.clone(),
        ),
        TestTokioRuntime::advance_after_run(1_000, 3, 11_000),
        AsyncGatewayTimeouts::default(),
    )?;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        boundary,
        Some(canary_for(launch.binding(), &owner, command_id.as_str())?),
        1_000,
    )?;
    let prepared =
        host.prepare_dispatch(entry_command(launch.binding(), command_id.as_str())?, 1_000)?;
    let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;
    assert_eq!(
        host.dispatch_admitted_for_test(prepared, capability, evidence, 1_000)?,
        DispatchOutcome::Unknown
    );
    assert_eq!(counters.dispatch.load(Ordering::SeqCst), 1);
    let journal = CommandJournal::open(journal_path(&launch))?;
    assert!(matches!(
        journal.receipt(&command_id).map(|receipt| &receipt.state),
        Some(CommandState::Unknown { reason }) if reason == "gateway_result_unknown"
    ));
    Ok(())
}

#[test]
fn sealed_admission_rechecks_gateway_version_and_flags_before_send()
-> Result<(), Box<dyn std::error::Error>> {
    for drift in ["version", "flags"] {
        let directory = tempfile::tempdir()?;
        let launch = launch(&directory, "LIVE")?;
        let owner = owner(launch.binding())?;
        let command_id = CommandId::new(format!("sealed_gateway_{drift}"))?;
        let dispatch_calls = Arc::new(AtomicUsize::new(0));
        let mut host = NodeSafetyHost::open_for_test(
            &launch,
            owner.clone(),
            FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
                .with_counter(Arc::clone(&dispatch_calls)),
            Some(canary_for(launch.binding(), &owner, command_id.as_str())?),
            1_000,
        )?;
        let prepared =
            host.prepare_dispatch(entry_command(launch.binding(), command_id.as_str())?, 1_000)?;
        let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;
        let snapshot = &mut host.gateway_mut_for_test().capability;
        if drift == "version" {
            snapshot.version = snapshot.version.saturating_add(1);
        } else {
            snapshot.flags.remove(CapabilityFlags::PLACE_LIMIT);
        }

        assert!(matches!(
            host.dispatch_admitted_for_test(prepared, capability, evidence, 1_001),
            Err(SafeHostError::GatewayApi(
                venue_gateway_api::GatewayApiError::CapabilityScope
                    | venue_gateway_api::GatewayApiError::CapabilityDenied
            ))
        ));
        assert_eq!(dispatch_calls.load(Ordering::SeqCst), 0);
        let journal = CommandJournal::open(journal_path(&launch))?;
        assert!(matches!(
            journal.receipt(&command_id).map(|receipt| &receipt.state),
            Some(CommandState::Rejected { reason }) if reason == "dispatch_revalidation_failed"
        ));
    }
    Ok(())
}

#[test]
fn unsupported_signed_order_family_cannot_be_opened_by_capability_flags()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
            .with_unsupported_regular(),
        Some(canary_for(
            launch.binding(),
            &owner,
            "command_unsupported_family",
        )?),
        1_000,
    )?;

    assert!(matches!(
        host.prepare_dispatch(
            entry_command(launch.binding(), "command_unsupported_family")?,
            1_000
        ),
        Err(SafeHostError::UnsupportedOrderFamily)
    ));
    let journal = CommandJournal::open(journal_path(&launch))?;
    assert_eq!(journal.commands().count(), 0);
    Ok(())
}

#[test]
fn writer_renewal_revokes_a_prepared_dispatch_before_gateway_call()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let command = entry_command(launch.binding(), "command_stale_prepared")?;
    let command_id = command.command_id().clone();
    let counter = Arc::new(AtomicUsize::new(0));
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
            .with_counter(Arc::clone(&counter)),
        Some(canary_for(
            launch.binding(),
            &owner,
            "command_stale_prepared",
        )?),
        1_000,
    )?;
    let prepared = host.prepare_dispatch(command, 1_000)?;
    let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;
    host.renew_writer(2_000)?;

    assert!(matches!(
        host.dispatch_admitted_for_test(prepared, capability, evidence, 2_000),
        Err(SafeHostError::PreparedStale)
    ));
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    let journal = CommandJournal::open(journal_path(&launch))?;
    assert!(matches!(
        journal.receipt(&command_id).map(|receipt| &receipt.state),
        Some(CommandState::Rejected { .. })
    ));
    Ok(())
}

#[test]
fn stop_requires_new_signed_zero_order_proof_and_retains_position_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut stop_readback = ReadbackPlan::recovery(ReadbackResolution::Rejected("not_applicable"));
    stop_readback.observed_ms = 1_100;
    stop_readback.nonzero_position = true;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::initial(), stop_readback],
        ),
        Some(canary(launch.binding(), &owner)?),
        1_000,
    )?;
    apply_control(
        &mut host,
        launch.binding(),
        ControlAction::Stop,
        "stop-with-custody",
        1_000,
    )?;
    let completion = host.complete_control(1_200)?;

    assert_eq!(completion.action, ControlAction::Stop);
    assert!(completion.symbol_custody_retained);
    assert!(matches!(
        host.prepare_dispatch(
            entry_command(launch.binding(), "command_after_stop")?,
            1_200
        ),
        Err(SafeHostError::ControlLifecycle)
    ));
    Ok(())
}

#[test]
fn flatten_remains_stopping_until_newer_readback_is_orderless_and_flat()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut still_positioned = ReadbackPlan::recovery(ReadbackResolution::Absent);
    still_positioned.observed_ms = 1_100;
    still_positioned.nonzero_position = true;
    let flat = ReadbackPlan {
        connection_generation: 2,
        private_generation: 3,
        observed_ms: 1_300,
        resolution: ReadbackResolution::Absent,
        nonzero_position: false,
        omit_family: false,
    };
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::initial(), still_positioned, flat],
        ),
        Some(canary(launch.binding(), &owner)?),
        1_000,
    )?;
    apply_control(
        &mut host,
        launch.binding(),
        ControlAction::Flatten,
        "flatten-until-flat",
        1_000,
    )?;

    assert!(matches!(
        host.complete_control(1_200),
        Err(SafeHostError::ControlNotProven)
    ));
    let completion = host.complete_control(1_400)?;

    assert_eq!(completion.action, ControlAction::Flatten);
    assert!(!completion.symbol_custody_retained);
    Ok(())
}

#[test]
fn incomplete_family_readback_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut incomplete = ReadbackPlan::initial();
    incomplete.omit_family = true;

    let result = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![incomplete]),
        Some(canary(launch.binding(), &owner)?),
        1_000,
    );

    assert!(matches!(result, Err(SafeHostError::ReadbackFamilies)));
    Ok(())
}

#[test]
fn live_host_without_canary_can_reconcile_and_stop_but_cannot_add_risk()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![
                ReadbackPlan::initial(),
                ReadbackPlan {
                    connection_generation: 1,
                    private_generation: 2,
                    observed_ms: 1_100,
                    resolution: ReadbackResolution::Absent,
                    nonzero_position: false,
                    omit_family: false,
                },
            ],
        ),
        None,
        1_000,
    )?;

    assert!(matches!(
        host.prepare_dispatch(entry_command(launch.binding(), "command_no_canary")?, 1_000),
        Err(SafeHostError::CanaryEvidence)
    ));
    apply_control(
        &mut host,
        launch.binding(),
        ControlAction::Stop,
        "stop-without-canary",
        1_000,
    )?;
    assert!(!host.complete_control(1_200)?.symbol_custody_retained);
    Ok(())
}

#[test]
fn fake_rejected_readback_result_is_terminal_without_resubmit()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let command = entry_command(launch.binding(), "command_rejected")?;
    let command_id = command.command_id().clone();
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![
                ReadbackPlan::initial(),
                ReadbackPlan {
                    connection_generation: 2,
                    private_generation: 2,
                    observed_ms: 1_100,
                    resolution: ReadbackResolution::Rejected("venue_rejected"),
                    nonzero_position: false,
                    omit_family: false,
                },
            ],
        )
        .with_dispatches(vec![GatewayDispatchResult::Unknown]),
        Some(canary_for(launch.binding(), &owner, "command_rejected")?),
        1_000,
    )?;
    let prepared = host.prepare_dispatch(command, 1_000)?;
    let (capability, evidence) = admitted_capability(launch.binding(), &prepared, 1_000)?;
    assert_eq!(
        host.dispatch_admitted_for_test(prepared, capability, evidence, 1_000)?,
        DispatchOutcome::Unknown
    );
    host.recover_unknowns(1_200)?;
    let journal = CommandJournal::open(journal_path(&launch))?;
    assert!(matches!(
        journal.receipt(&command_id).map(|receipt| &receipt.state),
        Some(CommandState::Rejected { reason }) if reason == "venue_rejected"
    ));
    Ok(())
}

#[test]
fn pause_and_resume_require_durable_actor_receipts_and_survive_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let command = entry_command(launch.binding(), "command_after_resume")?;
    let mut first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]),
        Some(canary_for(
            launch.binding(),
            &owner,
            "command_after_resume",
        )?),
        1_000,
    )?;
    apply_control(
        &mut first,
        launch.binding(),
        ControlAction::Pause,
        "pause-1",
        1_000,
    )?;
    assert!(matches!(
        first.prepare_dispatch(command.clone(), 1_000),
        Err(SafeHostError::ControlLifecycle)
    ));
    drop(first);

    let mut reopened = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::recovery(ReadbackResolution::Absent)],
        ),
        None,
        12_000,
    )?;
    assert!(matches!(
        reopened.prepare_dispatch(command.clone(), 12_000),
        Err(SafeHostError::ControlLifecycle)
    ));
    apply_control(
        &mut reopened,
        launch.binding(),
        ControlAction::Resume,
        "resume-1",
        12_000,
    )?;
    let _prepared = reopened.prepare_dispatch(command, 12_000)?;
    Ok(())
}

#[test]
fn crash_after_control_acceptance_reissues_exact_turn_without_opening_risk()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]),
        Some(canary_for(launch.binding(), &owner, "never_dispatched")?),
        1_000,
    )?;
    let _unapplied = first.accept_control_command(
        control_request(launch.binding(), ControlAction::Pause, "pause-crash"),
        1_000,
    )?;
    drop(first);

    let mut reopened = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::recovery(ReadbackResolution::Absent)],
        ),
        None,
        12_000,
    )?;
    assert!(matches!(
        reopened.prepare_dispatch(entry_command(launch.binding(), "never_dispatched")?, 12_000,),
        Err(SafeHostError::ControlLifecycle)
    ));
    let recovered = reopened
        .recovered_control_turn()?
        .ok_or("missing recovered control turn")?;
    assert_eq!(recovered.request().request_id, "pause-crash");
    let receipt = recovered.persisted(9, DIGEST, 12_000)?;
    let _ = reopened.apply_control_receipt(receipt)?;
    assert!(matches!(
        reopened.accept_control_command(
            control_request(launch.binding(), ControlAction::Pause, "pause-crash"),
            12_000,
        ),
        Err(SafeHostError::Supervision(
            crate::SupervisionError::DuplicateRequest
        ))
    ));
    Ok(())
}

#[test]
fn exact_command_bound_canary_is_required_before_wal() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]),
        None,
        1_000,
    )?;
    let turn = host.accept_canary_control(
        CanaryControlRequest {
            request_id: "canary-command-a".to_owned(),
            evidence: canary_for(launch.binding(), &owner, "command_a")?,
        },
        1_000,
    )?;
    let receipt = turn.persisted(1, DIGEST, 1_000)?;
    let _ = host.apply_canary_receipt(receipt)?;

    assert!(matches!(
        host.prepare_dispatch(entry_command(launch.binding(), "command_b")?, 1_000),
        Err(SafeHostError::CanaryEvidence)
    ));
    let _prepared = host.prepare_dispatch(entry_command(launch.binding(), "command_a")?, 1_000)?;
    Ok(())
}

#[test]
fn stale_recovered_generation_fails_before_writer_activation()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]),
        None,
        1_000,
    )?;
    apply_control(
        &mut first,
        launch.binding(),
        ControlAction::Pause,
        "pause-generation-floor",
        1_000,
    )?;
    drop(first);
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let result = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()])
            .with_connect_counter(Arc::clone(&connect_calls)),
        None,
        12_000,
    );

    assert!(matches!(result, Err(SafeHostError::ReadbackScope)));
    assert_eq!(connect_calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn corrupt_complete_control_record_fails_before_gateway_connect()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]),
        None,
        1_000,
    )?;
    drop(first);
    let mut journal = OpenOptions::new()
        .append(true)
        .open(supervision_path(&launch))?;
    journal.write_all(b"{}\n")?;
    journal.sync_all()?;
    drop(journal);
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let result = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::recovery(ReadbackResolution::Absent)],
        )
        .with_connect_counter(Arc::clone(&connect_calls)),
        None,
        12_000,
    );

    assert!(matches!(
        result,
        Err(SafeHostError::Supervision(
            crate::SupervisionError::CorruptJournal
        ))
    ));
    assert_eq!(connect_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[test]
fn incomplete_control_tail_is_truncated_then_recovered_before_connect()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]),
        None,
        1_000,
    )?;
    drop(first);
    let mut journal = OpenOptions::new()
        .append(true)
        .open(supervision_path(&launch))?;
    journal.write_all(b"{\"crash_tail\"")?;
    journal.sync_all()?;
    drop(journal);
    let connect_calls = Arc::new(AtomicUsize::new(0));
    let reopened = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan::recovery(ReadbackResolution::Absent)],
        )
        .with_connect_counter(Arc::clone(&connect_calls)),
        None,
        12_000,
    )?;

    assert_eq!(connect_calls.load(Ordering::SeqCst), 1);
    drop(reopened);
    Ok(())
}

#[test]
fn wrong_control_scope_and_config_generation_are_rejected_deterministically()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]),
        None,
        1_000,
    )?;
    let mut wrong_scope = control_request(launch.binding(), ControlAction::Pause, "wrong-scope");
    wrong_scope.instance_id = "other_instance".to_owned();
    assert!(matches!(
        host.accept_control_command(wrong_scope, 1_000),
        Err(SafeHostError::Supervision(
            crate::SupervisionError::RequestScope
        ))
    ));
    let mut stale_epoch = control_request(launch.binding(), ControlAction::Pause, "stale-epoch");
    stale_epoch.expected_config_epoch = 2;
    assert!(matches!(
        host.accept_control_command(stale_epoch, 1_000),
        Err(SafeHostError::Supervision(
            crate::SupervisionError::RequestScope
        ))
    ));
    Ok(())
}

#[test]
fn completed_stop_receipt_restores_stopped_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut first = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        FakeGateway::new(
            launch.binding().clone(),
            vec![
                ReadbackPlan::initial(),
                ReadbackPlan::recovery(ReadbackResolution::Absent),
            ],
        ),
        None,
        1_000,
    )?;
    apply_control(
        &mut first,
        launch.binding(),
        ControlAction::Stop,
        "durable-stop",
        1_000,
    )?;
    let completion = first.complete_control(12_000)?;
    assert_eq!(completion.request_id, "durable-stop");
    completion.receipt.validate()?;
    drop(first);

    let mut reopened = NodeSafetyHost::open_for_test(
        &launch,
        owner,
        FakeGateway::new(
            launch.binding().clone(),
            vec![ReadbackPlan {
                connection_generation: 3,
                private_generation: 3,
                observed_ms: 22_000,
                resolution: ReadbackResolution::Absent,
                nonzero_position: false,
                omit_family: false,
            }],
        ),
        None,
        23_000,
    )?;
    assert!(matches!(
        reopened.prepare_dispatch(
            entry_command(launch.binding(), "after-durable-stop")?,
            23_000,
        ),
        Err(SafeHostError::ControlLifecycle)
    ));
    assert!(matches!(
        reopened.complete_control(23_000),
        Err(SafeHostError::ControlLifecycle)
    ));
    Ok(())
}
