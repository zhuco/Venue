use std::{
    collections::{BTreeSet, VecDeque},
    ffi::OsString,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use rust_decimal::Decimal;
use tempfile::TempDir;
use venue_domain::domain::{
    CommandId, ExecutionCommand, OrderCommand, OrderOwner, OrderPurpose, OrderSide, PositionSide,
    Price,
};
use venue_execution::{CommandJournal, CommandState, WriterLeaseError};
use venue_gateway_api::{
    CapabilityFlags, CapabilitySnapshot, GatewayBinding, GatewayMode, VenueId,
};
use venue_runtime::{AccountKey, StrategyBinding, StrategyInstanceKey, StrategyKind};

use crate::{
    CanaryEvidence, ControlAction, ControlEvidence, DispatchOutcome, FamilyReadbackCoverage,
    GatewayAcknowledgement, GatewayDispatchResult, NodeLaunch, NodeSafetyHost, PhysicalGateway,
    ReadbackCommandState, SafeHostError, SignedCommandReadback, SignedReadbackReceipt,
    SignedReadbackRequest, safe_host::TestCrashPoint,
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
    unsupported_regular: bool,
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
            unsupported_regular: false,
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

    fn with_unsupported_regular(mut self) -> Self {
        self.unsupported_regular = true;
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

    fn signed_readback(
        &mut self,
        request: &SignedReadbackRequest,
    ) -> Result<SignedReadbackReceipt, Self::Error> {
        let plan = self.readbacks.pop_front().ok_or(())?;
        let regular = if self.unsupported_regular {
            FamilyReadbackCoverage::unsupported(venue_domain::domain::NativeOrderFamily::UmOrder)
        } else {
            FamilyReadbackCoverage::complete(venue_domain::domain::NativeOrderFamily::UmOrder)
        };
        let mut coverage = vec![
            regular,
            FamilyReadbackCoverage::complete(
                venue_domain::domain::NativeOrderFamily::UmConditional,
            ),
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
                    ReadbackResolution::Accepted(venue_order_id) => {
                        ReadbackCommandState::Accepted {
                            venue_order_id: venue_order_id.to_owned(),
                        }
                    }
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
    assert!(matches!(
        first.dispatch_with_crash(prepared, 1_000, TestCrashPoint::AfterSubmitted),
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
    assert!(matches!(
        first.dispatch_with_crash(prepared, 1_000, TestCrashPoint::AfterGatewayResult),
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

    assert_eq!(
        host.dispatch_prepared(prepared, 1_000)?,
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
fn capability_binding_mismatch_is_rejected_before_wal() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let launch = launch(&directory, "LIVE")?;
    let owner = owner(launch.binding())?;
    let mut gateway = FakeGateway::new(launch.binding().clone(), vec![ReadbackPlan::initial()]);
    gateway.capability.binding.mode = GatewayMode::Test;
    let mut host = NodeSafetyHost::open_for_test(
        &launch,
        owner.clone(),
        gateway,
        Some(canary_for(launch.binding(), &owner, "command_wrong_cap")?),
        1_000,
    )?;

    assert!(matches!(
        host.prepare_dispatch(entry_command(launch.binding(), "command_wrong_cap")?, 1_000),
        Err(SafeHostError::GatewayApi(_))
    ));
    let journal = CommandJournal::open(journal_path(&launch))?;
    assert!(!journal.has_unresolved());
    assert_eq!(journal.commands().count(), 0);
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
    host.renew_writer(2_000)?;

    assert!(matches!(
        host.dispatch_prepared(prepared, 2_000),
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
    let evidence = ControlEvidence::new(
        launch.binding().clone(),
        ControlAction::Stop,
        &owner,
        1_000,
        DIGEST,
    )?;

    host.request_control(evidence, 1_000)?;
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
    let evidence = ControlEvidence::new(
        launch.binding().clone(),
        ControlAction::Flatten,
        &owner,
        1_000,
        DIGEST,
    )?;
    host.request_control(evidence, 1_000)?;

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
    host.request_control(
        ControlEvidence::new(
            launch.binding().clone(),
            ControlAction::Stop,
            &owner,
            1_000,
            DIGEST,
        )?,
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
    assert_eq!(
        host.dispatch_prepared(prepared, 1_000)?,
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
