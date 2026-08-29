use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;

use super::stage7_resident::{exposure_poll_phase_allows, scheduled_private_readback_allowed};
use super::*;
use crate::{
    config::ExposureTakeProfitConfig,
    domain::{
        AccountBalance, AccountRiskSnapshot, Amount, Asset, Fill, Instrument, LegRiskSnapshot,
        MarketKind, MarketReduceCommand, OrderPurpose, RiskSourceStatus,
    },
    exchange::grid::{GridOrderFamilyReadback, GridRiskReadback, HedgedGridMutationClient},
    strategy::hedged_grid::{
        ExposureGuardState, GridEpoch, GridInventory, GridOrderRole, HedgedGridParams,
        InventoryRecoveryState,
    },
};

#[derive(Clone, Copy)]
enum ReductionOutcome {
    Accepted,
    Rejected,
    Unknown,
}

#[derive(Default)]
struct MutationCalls {
    reductions: Vec<MarketReduceCommand>,
    placements: usize,
    markets: usize,
    cancellations: usize,
}

#[derive(Clone)]
struct ExposureMutationClient {
    outcome: ReductionOutcome,
    calls: Arc<Mutex<MutationCalls>>,
}

impl HedgedGridMutationClient for ExposureMutationClient {
    fn place_limit_post_only(
        &self,
        _command: &crate::domain::OrderCommand,
    ) -> Result<String, GridVenueError> {
        self.calls
            .lock()
            .map_err(|_| GridVenueError::PrivateReadbackRequired)?
            .placements += 1;
        Err(GridVenueError::PrivateReadbackRequired)
    }

    fn place_market(&self, _command: &MarketOrderCommand) -> Result<String, GridVenueError> {
        self.calls
            .lock()
            .map_err(|_| GridVenueError::PrivateReadbackRequired)?
            .markets += 1;
        Err(GridVenueError::PrivateReadbackRequired)
    }

    fn place_market_reduce(&self, command: &MarketReduceCommand) -> Result<String, GridVenueError> {
        self.calls
            .lock()
            .map_err(|_| GridVenueError::PrivateReadbackRequired)?
            .reductions
            .push(command.clone());
        match self.outcome {
            ReductionOutcome::Accepted => Ok("venue-risk-reduce-1".to_owned()),
            ReductionOutcome::Rejected => Err(GridVenueError::Gate(
                crate::exchange::gate::GateError::Rejected {
                    label: "risk reduction rejected".to_owned(),
                },
            )),
            ReductionOutcome::Unknown => Err(GridVenueError::PrivateReadbackRequired),
        }
    }

    fn cancel_by_client_id(&self, _command: &CancelCommand) -> Result<String, GridVenueError> {
        self.calls
            .lock()
            .map_err(|_| GridVenueError::PrivateReadbackRequired)?
            .cancellations += 1;
        Err(GridVenueError::PrivateReadbackRequired)
    }
}

struct ExposureVenue {
    instrument: Instrument,
    outcome: ReductionOutcome,
    calls: Arc<Mutex<MutationCalls>>,
    public_ready: Arc<AtomicBool>,
}

impl HedgedGridVenue for ExposureVenue {
    fn exchange(&self) -> &'static str {
        "gate"
    }

    fn instrument(&self) -> &Instrument {
        &self.instrument
    }

    fn minimum_quantity(&self) -> Decimal {
        Decimal::ONE
    }

    fn verify_current_instrument_rules(&mut self) -> Result<(), GridVenueError> {
        Ok(())
    }

    fn best_bid_ask(&self, _now_ms: u64) -> Result<(Price, Price), GridVenueError> {
        if !self.public_ready.load(Ordering::SeqCst) {
            return Err(GridVenueError::PublicNotReady);
        }
        Ok((
            Price::new(Decimal::new(99, 2)).map_err(|_| GridVenueError::PublicPayload)?,
            Price::new(Decimal::new(101, 2)).map_err(|_| GridVenueError::PublicPayload)?,
        ))
    }

    fn readback(&mut self) -> Result<GridVenueReadback, GridVenueError> {
        ordinary_readback().map_err(|_| GridVenueError::PrivateReadbackRequired)
    }

    fn risk_readback(
        &mut self,
        account: &str,
        private_generation: u64,
    ) -> Result<GridRiskReadback, GridVenueError> {
        let observed_at_ms = current_time_ms().map_err(|_| GridVenueError::Clock)?;
        let currency = Asset::new("USDT").map_err(|_| GridVenueError::PrivateReadbackRequired)?;
        Ok(GridRiskReadback {
            raw_private_payloads: vec![
                "{\"apiKey\":\"shadow-secret-key\",\"signature\":\"shadow-secret-signature\"}"
                    .to_owned(),
            ],
            account: AccountRiskSnapshot {
                exchange: "gate".to_owned(),
                account: account.to_owned(),
                risk_currency: currency.clone(),
                account_equity: Decimal::new(20, 0),
                private_generation,
                observed_at_ms,
                source_status: RiskSourceStatus::Complete,
            },
            legs: vec![LegRiskSnapshot {
                symbol: self.instrument.symbol.clone(),
                position_side: PositionSide::Long,
                quantity: Decimal::new(60, 0),
                mark_price: Price::new(Decimal::ONE)
                    .map_err(|_| GridVenueError::PrivateReadbackRequired)?,
                contract_multiplier: Decimal::ONE,
                notional: Decimal::new(60, 0),
                unrealized_pnl: Decimal::new(2, 0),
                risk_currency: currency,
                private_generation,
                observed_at_ms,
            }],
        })
    }

    fn connect_private_stream(&mut self) -> Result<(), GridVenueError> {
        Ok(())
    }

    fn next_private_event(&mut self) -> Result<Option<GridPrivateEvent>, GridVenueError> {
        Ok(None)
    }

    fn reset_private_stream(&mut self) {}

    fn mutation_client(&self) -> Arc<dyn HedgedGridMutationClient> {
        Arc::new(ExposureMutationClient {
            outcome: self.outcome,
            calls: self.calls.clone(),
        })
    }

    fn order_by_client_id(&mut self, client_order_id: &str) -> Result<Order, GridVenueError> {
        match self.outcome {
            ReductionOutcome::Accepted => {
                let command = self
                    .calls
                    .lock()
                    .map_err(|_| GridVenueError::PrivateReadbackRequired)?
                    .reductions
                    .last()
                    .cloned()
                    .ok_or(GridVenueError::PrivateReadbackRequired)?;
                terminal_reduction_order(&command, client_order_id)
                    .map_err(|_| GridVenueError::PrivateReadbackRequired)
            }
            ReductionOutcome::Rejected => Err(GridVenueError::Gate(
                crate::exchange::gate::GateError::OrderAbsent,
            )),
            ReductionOutcome::Unknown => Err(GridVenueError::PrivateReadbackRequired),
        }
    }

    fn verify_post_only_order(&mut self, _client_order_id: &str) -> Result<(), GridVenueError> {
        Ok(())
    }
}

struct ExposureHarness {
    _temporary: tempfile::TempDir,
    artifacts_root: std::path::PathBuf,
    binding: HedgedGridBinding,
    settings: crate::runtime::hedged_grid::ExposureRuntimeSettings,
    checkpoint: Stage7GridCheckpoint,
    checkpoint_store: ProjectionStore,
    commands: CommandJournal,
    evidence: PrivateEvidenceJournal,
    shadow_evidence: crate::runtime::hedged_grid::ExposureShadowEvidenceJournal,
    authority: WriterLeaseAuthority,
    writer: Option<WriterSession>,
    venue: ExposureVenue,
}

impl ExposureHarness {
    fn new(outcome: ReductionOutcome) -> Result<Self, Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let artifacts_root = temporary.path().to_path_buf();
        let binding = binding()?;
        let mut state = HedgedGridState::new_with_params(
            binding.clone(),
            HedgedGridParams::fixed_release(Asset::new("USDT")?, 3)?,
        )?;
        let _ = state.observe_inventory(GridInventory {
            private_generation: 9,
            private_observed_at_ms: 900,
            mark_price: Price::new(Decimal::ONE)?,
            long_quantity: Decimal::new(15, 0),
            short_quantity: Decimal::new(15, 0),
        })?;
        let _ = state.install_epoch(GridEpoch {
            epoch: 7,
            anchor_price: Price::new(Decimal::ONE)?,
            step: Price::new(Decimal::new(1, 2))?,
            grid_quantity: Decimal::new(5, 0),
            passive_book_fallback: None,
        })?;
        state.inventory_recovery = InventoryRecoveryState::AwaitingNextOwnedFill {
            armed_generation: 9,
        };
        let settings =
            crate::runtime::hedged_grid::ExposureRuntimeSettings::try_from(exposure_config())?;
        let checkpoint = Stage7GridCheckpoint {
            schema_version: 1,
            binding: binding.clone(),
            state,
            private_generation: 9,
            exposure_guard: Some(ExposureGuardState::new(
                binding.clone(),
                settings.guard.clone(),
            )?),
            pending_exposure_reduction: None,
            fill_history_start_ms: 1,
            order_health_fenced: false,
            last_order_health_checked_at_ms: 0,
        };
        let checkpoint_store = ProjectionStore::new(artifacts_root.join(CHECKPOINT_FILE));
        checkpoint_store.save(&checkpoint)?;
        let commands = CommandJournal::open(artifacts_root.join(COMMAND_FILE))?;
        let evidence = PrivateEvidenceJournal::open(artifacts_root.join(PRIVATE_EVIDENCE_FILE))?;
        let shadow_evidence = crate::runtime::hedged_grid::ExposureShadowEvidenceJournal::open(
            artifacts_root.join(crate::runtime::hedged_grid::EXPOSURE_SHADOW_EVIDENCE_FILE),
        )?;
        let authority = WriterLeaseAuthority::open(
            artifacts_root.join(WRITER_FILE),
            stage7_writer_scope(&binding),
        )?;
        let calls = Arc::new(Mutex::new(MutationCalls::default()));
        let venue = ExposureVenue {
            instrument: instrument()?,
            outcome,
            calls,
            public_ready: Arc::new(AtomicBool::new(true)),
        };
        Ok(Self {
            _temporary: temporary,
            artifacts_root,
            binding,
            settings,
            checkpoint,
            checkpoint_store,
            commands,
            evidence,
            shadow_evidence,
            authority,
            writer: None,
            venue,
        })
    }

    fn poll(&mut self) -> Result<bool, Stage7GridError> {
        self.poll_with_shadow(false)
    }

    fn poll_with_shadow(&mut self, shadow_only: bool) -> Result<bool, Stage7GridError> {
        stage7_exposure::poll_exposure_take_profit(
            &self.settings,
            &mut self.checkpoint,
            &self.checkpoint_store,
            &mut self.commands,
            &mut self.evidence,
            &mut self.shadow_evidence,
            &mut self.venue,
            &self.authority,
            &mut self.writer,
            &self.binding,
            shadow_only,
            current_time_ms().map_err(|_| Stage7GridError::Clock)?,
            None,
        )
    }

    fn only_command(&self) -> Result<MarketReduceCommand, Box<dyn std::error::Error>> {
        let calls = self
            .venue
            .calls
            .lock()
            .map_err(|_| "mutation call lock poisoned")?;
        if calls.reductions.len() != 1 {
            return Err("expected exactly one market reduction".into());
        }
        Ok(calls.reductions[0].clone())
    }

    fn assert_only_one_reduction(&self) -> Result<(), Box<dyn std::error::Error>> {
        let calls = self
            .venue
            .calls
            .lock()
            .map_err(|_| "mutation call lock poisoned")?;
        assert_eq!(calls.reductions.len(), 1);
        assert_eq!(calls.placements, 0);
        assert_eq!(calls.markets, 0);
        assert_eq!(calls.cancellations, 0);
        Ok(())
    }

    fn assert_no_mutations(&self) -> Result<(), Box<dyn std::error::Error>> {
        let calls = self
            .venue
            .calls
            .lock()
            .map_err(|_| "mutation call lock poisoned")?;
        assert!(calls.reductions.is_empty());
        assert_eq!(calls.placements, 0);
        assert_eq!(calls.markets, 0);
        assert_eq!(calls.cancellations, 0);
        Ok(())
    }

    fn set_public_ready(&self, ready: bool) {
        self.venue.public_ready.store(ready, Ordering::SeqCst);
    }
}

#[test]
fn shadow_risk_evidence_is_hashed_deduplicated_and_never_mutates()
-> Result<(), Box<dyn std::error::Error>> {
    let mut harness = ExposureHarness::new(ReductionOutcome::Accepted)?;
    let grid_before = harness.checkpoint.state.clone();

    assert!(!harness.poll_with_shadow(true)?);
    assert_eq!(harness.checkpoint.state, grid_before);
    assert!(harness.writer.is_none());
    assert!(!harness.artifacts_root.join(COMMAND_FILE).exists());
    let first_records = harness.shadow_evidence.recover()?;
    assert_eq!(first_records.len(), 2);
    let private_records = harness.evidence.recover()?;
    assert!(first_records.iter().all(|record| {
        record.raw_evidence.iter().all(|reference| {
            private_records.iter().any(|raw| {
                raw.sequence == reference.sequence
                    && raw.generation == reference.generation
                    && raw.payload_sha256 == reference.payload_sha256
                    && raw.valid_hash()
            })
        })
    }));
    assert!(first_records.iter().any(|record| {
        record.position == GridPosition::Long
            && record.decision == crate::runtime::hedged_grid::ExposureShadowDecision::WouldReduce
            && record.reason == crate::runtime::hedged_grid::ExposureShadowReason::ThresholdBreached
            && record.exposure_notional_threshold == Decimal::new(60, 0)
            && record.unrealized_pnl_threshold == Decimal::ONE
    }));
    harness.assert_no_mutations()?;

    // The first post-trigger observation changes the decision to episode-suppressed and is
    // auditable. Later generations with identical normalized values and decision are deduped.
    assert!(!harness.poll_with_shadow(true)?);
    assert_eq!(harness.shadow_evidence.recover()?.len(), 3);
    assert!(!harness.poll_with_shadow(true)?);
    assert_eq!(harness.shadow_evidence.recover()?.len(), 3);
    harness.assert_no_mutations()?;
    let normalized = std::fs::read_to_string(
        harness
            .artifacts_root
            .join(crate::runtime::hedged_grid::EXPOSURE_SHADOW_EVIDENCE_FILE),
    )?;
    assert!(!normalized.contains("shadow-secret-key"));
    assert!(!normalized.contains("shadow-secret-signature"));
    Ok(())
}

#[test]
fn partial_risk_reduction_keeps_grid_and_awaiting_anchor_unchanged()
-> Result<(), Box<dyn std::error::Error>> {
    let mut harness = ExposureHarness::new(ReductionOutcome::Accepted)?;
    let grid_before = harness.checkpoint.state.clone();
    assert_grid_has_opening_and_closing(&grid_before);

    assert!(harness.poll()?);
    let command = harness.only_command()?;
    assert_eq!(command.quantity, Decimal::new(18, 0));
    assert_eq!(harness.checkpoint.state, grid_before);

    // A resident retry sees the durable pending episode and cannot submit a second command.
    assert!(harness.poll()?);
    harness.assert_only_one_reduction()?;

    let fill = reduction_fill(&command, Decimal::new(9, 0))?;
    assert_eq!(
        crate::runtime::hedged_grid::route_grid_fill(&fill),
        crate::runtime::hedged_grid::GridFillRoute::TakerInventoryOnly
    );
    let readback = settlement_readback(&command, Some(fill))?;
    let writer = harness.writer.as_ref().ok_or("missing writer")?;
    assert!(!stage7_exposure::settle_exposure_take_profit(
        &mut harness.checkpoint,
        &harness.checkpoint_store,
        &mut harness.commands,
        &mut harness.venue,
        &harness.authority,
        writer,
        &harness.binding,
        &readback,
        &harness.artifacts_root,
        13,
    )?);
    assert_eq!(harness.checkpoint.state, grid_before);
    assert_eq!(
        harness.checkpoint.state.inventory_recovery,
        InventoryRecoveryState::AwaitingNextOwnedFill {
            armed_generation: 9
        }
    );
    harness.assert_only_one_reduction()?;
    Ok(())
}

#[test]
fn resetting_grid_keeps_risk_polling_and_clears_only_the_settled_runtime_envelope()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(exposure_poll_phase_allows(GridPhase::ResettingGrid));
    assert!(exposure_poll_phase_allows(GridPhase::Running));
    assert!(!exposure_poll_phase_allows(GridPhase::Stopping));
    assert!(!exposure_poll_phase_allows(GridPhase::BlockedUnknown));

    let mut harness = ExposureHarness::new(ReductionOutcome::Accepted)?;
    harness
        .checkpoint
        .state
        .begin_reconciliation_reset(BTreeMap::new())?;
    let grid_before = harness.checkpoint.state.clone();
    assert!(harness.poll()?);
    let command = harness.only_command()?;
    let readback = settlement_readback(
        &command,
        Some(reduction_fill(&command, Decimal::new(9, 0))?),
    )?;
    let writer = harness.writer.as_ref().ok_or("missing writer")?;
    assert!(!stage7_exposure::settle_exposure_take_profit(
        &mut harness.checkpoint,
        &harness.checkpoint_store,
        &mut harness.commands,
        &mut harness.venue,
        &harness.authority,
        writer,
        &harness.binding,
        &readback,
        &harness.artifacts_root,
        13,
    )?);
    assert!(harness.checkpoint.pending_exposure_reduction.is_some());
    assert!(!stage7_exposure::settle_exposure_take_profit(
        &mut harness.checkpoint,
        &harness.checkpoint_store,
        &mut harness.commands,
        &mut harness.venue,
        &harness.authority,
        writer,
        &harness.binding,
        &readback,
        &harness.artifacts_root,
        14,
    )?);
    assert!(harness.checkpoint.pending_exposure_reduction.is_none());
    assert_eq!(harness.checkpoint.state, grid_before);
    harness.assert_only_one_reduction()?;
    Ok(())
}

#[test]
fn latched_risk_repair_yields_to_a_new_complete_owned_maker_fill()
-> Result<(), Box<dyn std::error::Error>> {
    let mut harness = ExposureHarness::new(ReductionOutcome::Accepted)?;
    assert!(harness.poll()?);
    let command = harness.only_command()?;
    let writer = harness.writer.as_ref().ok_or("missing writer")?;
    assert!(!stage7_exposure::settle_exposure_take_profit(
        &mut harness.checkpoint,
        &harness.checkpoint_store,
        &mut harness.commands,
        &mut harness.venue,
        &harness.authority,
        writer,
        &harness.binding,
        &settlement_readback(
            &command,
            Some(reduction_fill(&command, Decimal::new(9, 0))?),
        )?,
        &harness.artifacts_root,
        13,
    )?);

    let (key, intent) = harness
        .checkpoint
        .state
        .owned_orders
        .iter()
        .next()
        .map(|(key, intent)| (key.clone(), intent.clone()))
        .ok_or("missing owned grid order")?;
    let mut readback = ordinary_readback()?;
    readback.fills.push(GridVenueFill {
        client_order_id: FieldState::Known(client_order_id(&key)?.as_str().to_owned()),
        fill: Fill {
            execution_sequence: FieldState::Known(2),
            fill_id: "maker-after-risk-latched".to_owned(),
            order_id: "venue-maker-after-risk".to_owned(),
            symbol: harness.binding.symbol.clone(),
            side: intent.side,
            position_side: FieldState::Known(match key.position {
                GridPosition::Long => PositionSide::Long,
                GridPosition::Short => PositionSide::Short,
            }),
            quantity: intent.quantity,
            price: intent.price,
            fee: FieldState::Missing,
            realized_pnl: FieldState::Missing,
            maker: FieldState::Known(true),
            exchange_time_ms: Some(1_002),
        },
    });
    let grid_before = harness.checkpoint.state.clone();
    assert!(!stage7_exposure::settle_exposure_take_profit(
        &mut harness.checkpoint,
        &harness.checkpoint_store,
        &mut harness.commands,
        &mut harness.venue,
        &harness.authority,
        writer,
        &harness.binding,
        &readback,
        &harness.artifacts_root,
        14,
    )?);
    assert_eq!(harness.checkpoint.state, grid_before);
    assert!(harness.checkpoint.pending_exposure_reduction.is_some());
    harness.assert_only_one_reduction()?;
    Ok(())
}

#[test]
fn latched_risk_repair_waits_for_public_recovery_without_stopping_or_mutating()
-> Result<(), Box<dyn std::error::Error>> {
    assert!(!scheduled_private_readback_allowed(false, true));
    assert!(scheduled_private_readback_allowed(true, true));
    assert!(scheduled_private_readback_allowed(false, false));

    let mut harness = ExposureHarness::new(ReductionOutcome::Accepted)?;
    assert!(harness.poll()?);
    let command = harness.only_command()?;
    let writer = harness.writer.as_ref().ok_or("missing writer")?;
    let readback = settlement_readback(
        &command,
        Some(reduction_fill(&command, Decimal::new(9, 0))?),
    )?;
    assert!(!stage7_exposure::settle_exposure_take_profit(
        &mut harness.checkpoint,
        &harness.checkpoint_store,
        &mut harness.commands,
        &mut harness.venue,
        &harness.authority,
        writer,
        &harness.binding,
        &readback,
        &harness.artifacts_root,
        13,
    )?);
    let latched = harness.checkpoint.clone();

    harness.set_public_ready(false);
    assert_eq!(
        stage7_exposure::settle_exposure_take_profit_with_public_refresh(
            &mut harness.checkpoint,
            &harness.checkpoint_store,
            &mut harness.commands,
            &mut harness.venue,
            &harness.authority,
            writer,
            &harness.binding,
            &readback,
            &harness.artifacts_root,
            14,
            |_| Ok(false),
        )?,
        stage7_exposure::ExposureSettlement::PublicDeferred
    );
    assert_eq!(harness.checkpoint, latched);
    assert!(stage7_exposure::latched_exposure_repair_pending(
        &harness.checkpoint
    )?);
    harness.assert_only_one_reduction()?;

    assert_eq!(
        stage7_exposure::settle_exposure_take_profit_with_public_refresh(
            &mut harness.checkpoint,
            &harness.checkpoint_store,
            &mut harness.commands,
            &mut harness.venue,
            &harness.authority,
            writer,
            &harness.binding,
            &readback,
            &harness.artifacts_root,
            15,
            |_| Ok(false),
        )?,
        stage7_exposure::ExposureSettlement::PublicDeferred
    );
    assert_eq!(harness.checkpoint, latched);
    harness.assert_only_one_reduction()?;

    harness.set_public_ready(true);
    assert_eq!(
        stage7_exposure::settle_exposure_take_profit_with_public_refresh(
            &mut harness.checkpoint,
            &harness.checkpoint_store,
            &mut harness.commands,
            &mut harness.venue,
            &harness.authority,
            writer,
            &harness.binding,
            &readback,
            &harness.artifacts_root,
            16,
            |venue| {
                venue.public_ready.store(true, Ordering::SeqCst);
                Ok(true)
            },
        )?,
        stage7_exposure::ExposureSettlement::PrivateReadbackRequired
    );
    let calls = harness
        .venue
        .calls
        .lock()
        .map_err(|_| "mutation call lock poisoned")?;
    assert!(calls.placements + calls.cancellations > 0);
    Ok(())
}

#[test]
fn rejected_risk_reduction_is_terminal_without_grid_cancellation_or_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let mut harness = ExposureHarness::new(ReductionOutcome::Rejected)?;
    let grid_before = harness.checkpoint.state.clone();
    assert!(matches!(harness.poll(), Err(Stage7GridError::Rejected)));
    let command = harness.only_command()?;
    assert!(matches!(
        harness
            .commands
            .receipt(&command.command_id)
            .map(|receipt| &receipt.state),
        Some(CommandState::Rejected { .. })
    ));
    assert_eq!(harness.checkpoint.state, grid_before);
    assert!(harness.poll()?);
    harness.assert_only_one_reduction()?;

    let writer = harness.writer.as_ref().ok_or("missing writer")?;
    assert!(!stage7_exposure::settle_exposure_take_profit(
        &mut harness.checkpoint,
        &harness.checkpoint_store,
        &mut harness.commands,
        &mut harness.venue,
        &harness.authority,
        writer,
        &harness.binding,
        &settlement_readback(&command, None)?,
        &harness.artifacts_root,
        13,
    )?);
    assert_eq!(harness.checkpoint.state, grid_before);
    harness.assert_only_one_reduction()?;
    Ok(())
}

#[test]
fn unknown_risk_reduction_stays_pending_without_grid_cancellation_or_resubmit()
-> Result<(), Box<dyn std::error::Error>> {
    let mut harness = ExposureHarness::new(ReductionOutcome::Unknown)?;
    let grid_before = harness.checkpoint.state.clone();
    assert!(matches!(harness.poll(), Err(Stage7GridError::Unresolved)));
    let command = harness.only_command()?;
    assert!(matches!(
        harness
            .commands
            .receipt(&command.command_id)
            .map(|receipt| &receipt.state),
        Some(CommandState::Unknown { .. })
    ));
    assert_eq!(harness.checkpoint.state, grid_before);

    assert!(harness.poll()?);
    harness.assert_only_one_reduction()?;
    let writer = harness.writer.as_ref().ok_or("missing writer")?;
    assert!(stage7_exposure::settle_exposure_take_profit(
        &mut harness.checkpoint,
        &harness.checkpoint_store,
        &mut harness.commands,
        &mut harness.venue,
        &harness.authority,
        writer,
        &harness.binding,
        &settlement_readback(&command, None)?,
        &harness.artifacts_root,
        13,
    )?);
    assert_eq!(harness.checkpoint.state, grid_before);
    harness.assert_only_one_reduction()?;
    Ok(())
}

#[test]
fn startup_rejects_combined_guard_pending_action_review_and_command_tampering()
-> Result<(), Box<dyn std::error::Error>> {
    let mut harness = ExposureHarness::new(ReductionOutcome::Accepted)?;
    assert!(harness.poll()?);
    let valid = harness.checkpoint.clone();
    stage7_exposure::validate_exposure_checkpoint(&valid, &harness.binding)?;

    let rejects = |checkpoint: &Stage7GridCheckpoint| {
        matches!(
            stage7_exposure::validate_exposure_checkpoint(checkpoint, &harness.binding),
            Err(Stage7GridError::Checkpoint)
        )
    };

    let mut missing_pending = valid.clone();
    missing_pending.pending_exposure_reduction = None;
    assert!(rejects(&missing_pending));

    let mut wrong_lifecycle = valid.clone();
    wrong_lifecycle
        .exposure_guard
        .as_mut()
        .ok_or("missing exposure guard")?
        .long
        .state = crate::strategy::hedged_grid::ExposureEpisodeState::TriggerPersisted {
        risk_episode_id: valid
            .pending_exposure_reduction
            .as_ref()
            .ok_or("missing pending exposure")?
            .action
            .risk_episode_id
            .clone(),
    };
    assert!(rejects(&wrong_lifecycle));

    let mut wrong_episode = valid.clone();
    let pending = wrong_episode
        .pending_exposure_reduction
        .as_mut()
        .ok_or("missing pending exposure")?;
    pending.action.risk_episode_id = "etp-l-0000000000000000".to_owned();
    let command = pending
        .command
        .as_mut()
        .ok_or("missing reduction command")?;
    command.risk_episode_id = crate::domain::CommandId::new("etp-l-0000000000000000")?;
    command.command_id = crate::domain::CommandId::new("cmd-etp-l-0000000000000000")?;
    command.client_order_id = crate::domain::CommandId::new("ord-etp-l-0000000000000000")?;
    assert!(rejects(&wrong_episode));

    let mut wrong_action = valid.clone();
    wrong_action
        .pending_exposure_reduction
        .as_mut()
        .ok_or("missing pending exposure")?
        .action
        .reduce_ratio = Decimal::new(31, 2);
    assert!(rejects(&wrong_action));

    let mut wrong_review = valid.clone();
    wrong_review
        .pending_exposure_reduction
        .as_mut()
        .ok_or("missing pending exposure")?
        .review_leg
        .notional += Decimal::ONE;
    assert!(rejects(&wrong_review));

    let mut wrong_command = valid;
    let pending = wrong_command
        .pending_exposure_reduction
        .as_mut()
        .ok_or("missing pending exposure")?;
    pending
        .command
        .as_mut()
        .ok_or("missing reduction command")?
        .quantity = pending.review_leg.quantity;
    assert!(rejects(&wrong_command));
    Ok(())
}

fn exposure_config() -> ExposureTakeProfitConfig {
    ExposureTakeProfitConfig {
        enabled: true,
        shadow: false,
        position_equity_multiple: Decimal::new(3, 0),
        unrealized_pnl_equity_ratio: Decimal::new(5, 2),
        reduce_ratio: Decimal::new(30, 2),
        snapshot_interval_ms: 120_000,
        max_snapshot_age_ms: 3_000,
        rearm_clear_generations: 2,
    }
}

fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
    Ok(HedgedGridBinding {
        strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
        run_id: "primary".to_owned(),
        exchange: "gate".to_owned(),
        account: "usdt_futures".to_owned(),
        symbol: "DOGE/USDT".parse()?,
        config_version: "risk_test_v1".to_owned(),
        owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
    })
}

fn instrument() -> Result<Instrument, Box<dyn std::error::Error>> {
    let currency = Asset::new("USDT")?;
    Ok(Instrument {
        symbol: "DOGE/USDT".parse()?,
        market: MarketKind::LinearPerpetual,
        settlement_asset: Some(currency.clone()),
        generation: 1,
        price_tick: Price::new(Decimal::new(1, 4))?,
        quantity_step: Decimal::ONE,
        minimum_notional: Amount::new(currency, Decimal::ONE),
    })
}

fn ordinary_readback() -> Result<GridVenueReadback, Box<dyn std::error::Error>> {
    Ok(GridVenueReadback {
        raw_private_payloads: vec!["{\"orders\":[]}".to_owned()],
        order_family_readback: Some(GridOrderFamilyReadback::regular_only_adapter_profile(
            Vec::new(),
            vec!["[]".to_owned()],
        )?),
        balance: AccountBalance {
            asset: Asset::new("USDT")?,
            wallet_balance: Decimal::new(20, 0),
            available_balance: Decimal::new(20, 0),
            initial_margin: Decimal::ZERO,
            maintenance_margin: Decimal::ZERO,
        },
        hedge_position: true,
        positions: Vec::new(),
        orders: Vec::new(),
        fills: Vec::new(),
    })
}

fn settlement_readback(
    command: &MarketReduceCommand,
    fill: Option<Fill>,
) -> Result<GridVenueReadback, Box<dyn std::error::Error>> {
    let mut readback = ordinary_readback()?;
    if let Some(fill) = fill {
        readback.fills.push(GridVenueFill {
            fill,
            client_order_id: FieldState::Known(command.client_order_id.as_str().to_owned()),
        });
    }
    Ok(readback)
}

fn reduction_fill(
    command: &MarketReduceCommand,
    quantity: Decimal,
) -> Result<Fill, Box<dyn std::error::Error>> {
    Ok(Fill {
        execution_sequence: FieldState::Known(1),
        fill_id: "risk-partial-fill-1".to_owned(),
        order_id: "venue-risk-reduce-1".to_owned(),
        symbol: command.owner.symbol.clone(),
        side: command.side,
        position_side: FieldState::Known(command.position_side),
        quantity,
        price: Price::new(Decimal::ONE)?,
        fee: FieldState::Missing,
        realized_pnl: FieldState::Missing,
        maker: FieldState::Known(false),
        exchange_time_ms: Some(1_001),
    })
}

fn terminal_reduction_order(
    command: &MarketReduceCommand,
    client_order_id: &str,
) -> Result<Order, Box<dyn std::error::Error>> {
    Ok(Order {
        order_id: "venue-risk-reduce-1".to_owned(),
        client_order_id: FieldState::Known(client_order_id.to_owned()),
        symbol: command.owner.symbol.clone(),
        side: command.side,
        position_side: FieldState::Known(command.position_side),
        purpose: FieldState::Known(OrderPurpose::ExposureTakeProfit),
        state: OrderState::Cancelled,
        quantity: command.quantity,
        filled_quantity: Decimal::new(9, 0),
        limit_price: None,
        average_price: FieldState::Known(Price::new(Decimal::ONE)?),
        reduce_only: true,
    })
}

fn assert_grid_has_opening_and_closing(state: &HedgedGridState) {
    assert!(
        state
            .owned_orders
            .keys()
            .any(|key| key.role == GridOrderRole::Open)
    );
    assert!(
        state
            .owned_orders
            .keys()
            .any(|key| key.role == GridOrderRole::Close)
    );
}

fn current_time_ms() -> Result<u64, Box<dyn std::error::Error>> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}
