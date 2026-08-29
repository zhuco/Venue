use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;

use crate::{
    config::Config,
    domain::{
        Amount, Asset, CancelCommand, CommandId, FieldState, Instrument, MarketEvent, Order,
        OrderCommand, OrderOwner, OrderPurpose, OrderSide, OrderState, Position, PositionSide,
        Price, StopMarketFullPositionCommand,
    },
    exchange::{
        binance::{
            PrivateCredentials, PrivateError, PrivateRest, PrivateStreamSocket, PublicRest,
            native_symbol, normalize, parse_instrument,
        },
        binance_private::{self, ConditionalStrategyStatus, PrivateReadback},
    },
    execution::{
        AlgoProtectionCustodyInput, CanaryBinding, CanaryEvidenceBinding, CanaryEvidenceJournal,
        CanaryPosition, CanaryPreflightInput, CanaryRunBinding, CanaryRunState, CanarySnapshot,
        CanaryTerminalState, Capability, CapabilityBinding, CapabilityEvidenceStore,
        CapabilityProbe, CommandJournal, CustodyWriterRole, EmergencyDispatchState,
        EmergencyFlattenInput, EmergencyRiskEnvelope, ExecutionError, ExecutionReceipt,
        FlatReceipt, PostOnlyProbePreflight, ProbeExecutionState, ProbeKind, ProbePermitInput,
        ProtectionEvidence, ProtectionPreflight, ReadbackBatch, Reconciler, WRITER_LEASE_TTL_MS,
        WriterLeaseAuthority, WriterScope, authorize_canary_preflight, authorize_emergency_flatten,
        authorize_probe_permit, prove_algo_protection_custody, resolve_unknown_order_by_readback,
        sha256_hex, submit_cancel, submit_emergency_flatten, submit_post_only_probe,
        submit_protection_probe_entry, submit_stop_market_full_position,
    },
    market::{RawMarketRecord, RawSource},
    storage::Journal,
};

const PRIVATE_GENERATION: u64 = 1;
const EVIDENCE_TTL_MS: u64 = 5 * 60 * 1_000;
const PREFLIGHT_MAX_AGE_MS: u64 = 30_000;
const ALGO_VISIBILITY_RETRY_MS: u64 = 25;
pub(super) const MAINNET_CANARY_OWNER_SCOPE: &str = "manual_canary_mainnet";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceCanaryPhase {
    PlaceCancel,
    Protection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceCanaryRequest {
    pub phase: BinanceCanaryPhase,
    pub position_side: PositionSide,
    pub artifacts_root: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceCanaryReport {
    pub phase: BinanceCanaryPhase,
    pub symbol: crate::domain::Symbol,
    pub quantity: Decimal,
    pub entry_notional: Amount,
    pub terminal_flat: bool,
    pub evidence_path: PathBuf,
}

pub fn run_binance_canary(
    cfg: &Config,
    request: BinanceCanaryRequest,
) -> Result<BinanceCanaryReport, BinanceCanaryError> {
    if !request.artifacts_root.is_absolute()
        || !matches!(
            request.position_side,
            PositionSide::Long | PositionSide::Short
        )
    {
        return Err(BinanceCanaryError::Request);
    }
    let started_at_ms = wall_clock_ms()?;
    let phase_name = match request.phase {
        BinanceCanaryPhase::PlaceCancel => "place_cancel",
        BinanceCanaryPhase::Protection => "protection",
    };
    let symbol_dir = request
        .artifacts_root
        .join(native_symbol(&cfg.symbol).to_ascii_lowercase());
    fs::create_dir_all(&symbol_dir).map_err(BinanceCanaryError::Io)?;
    let _account_writer_root = super::stage7_writer_registry::acquire(
        &WriterScope {
            exchange: "binance".to_owned(),
            account: cfg.trading_account_id.clone(),
            symbol: cfg.symbol.clone(),
            owner_scope: MAINNET_CANARY_OWNER_SCOPE.to_owned(),
        },
        &request.artifacts_root,
    )
    .map_err(|error| BinanceCanaryError::WriterRegistry {
        reason: error.to_string(),
    })?;
    ensure_no_unfinished_runs(&symbol_dir, started_at_ms)?;
    if request.phase == BinanceCanaryPhase::Protection {
        super::canary_sequence_runtime::ensure_sol_pending(
            &request.artifacts_root,
            &cfg.trading_account_id,
            &cfg.symbol,
        )?;
    }
    let run_id = format!("{phase_name}_{started_at_ms}");
    let run_dir = symbol_dir.join(&run_id);

    let credentials = PrivateCredentials::from_environment()?;
    let private = PrivateRest::production(
        credentials,
        cfg.binance
            .as_ref()
            .ok_or(BinanceCanaryError::Request)?
            .account_binding,
    )?;
    super::canary_recovery_runtime::validate_terminal_recovery_receipts(
        &symbol_dir,
        &private,
        wall_clock_ms()?,
    )?;
    fs::create_dir(&run_dir).map_err(BinanceCanaryError::Io)?;
    let public = PublicRest::production()?;
    verify_private_stream(&private)?;

    let exchange_info = public.exchange_info()?;
    let instrument = parse_instrument(&exchange_info, cfg.symbol.clone(), PRIVATE_GENERATION)?;
    let depth_payload = public.depth_snapshot(&cfg.symbol, 5)?;
    let book = parse_book(&cfg.symbol, started_at_ms, &depth_payload)?;
    let _ = best_prices(&book)?;

    let owner_scope = MAINNET_CANARY_OWNER_SCOPE.to_owned();
    let owner = OrderOwner {
        strategy_instance_id: "manual_canary".to_owned(),
        run_id: run_id.clone(),
        exchange: "binance".to_owned(),
        account: cfg.trading_account_id.clone(),
        symbol: cfg.symbol.clone(),
        purpose: OrderPurpose::Entry,
    };
    let canary_binding = CanaryBinding {
        exchange: owner.exchange.clone(),
        account: owner.account.clone(),
        symbol: cfg.symbol.clone(),
        owner: owner.clone(),
        release_id: "stage4_manual_canary_v1".to_owned(),
        position_side: request.position_side,
    };

    let mut facts = Journal::open(run_dir.join("facts.jsonl"))?;
    let mut reconciler = Reconciler::recover(&facts)?;
    let first_at_ms = wall_clock_ms()?;
    let first = private.readback(&cfg.symbol)?;
    ensure_no_open_algos(&private, &cfg.symbol)?;
    accept_readback(&mut facts, &mut reconciler, &first, first_at_ms)?;
    thread::sleep(Duration::from_millis(2));
    let second_at_ms = wall_clock_ms()?;
    let second = private.readback(&cfg.symbol)?;
    ensure_no_open_algos(&private, &cfg.symbol)?;
    accept_readback(&mut facts, &mut reconciler, &second, second_at_ms)?;
    let snapshots = [
        canary_snapshot(&canary_binding, &first, first_at_ms, &instrument)?,
        canary_snapshot(&canary_binding, &second, second_at_ms, &instrument)?,
    ];

    let api_key = crate::credential_env::required("BINANCE_API_KEY")
        .map_err(|_| BinanceCanaryError::Request)?;
    let capability_binding = CapabilityBinding {
        exchange: "binance".to_owned(),
        account_binding: "portfolio_margin_um".to_owned(),
        symbol: cfg.symbol.to_string(),
        api_key_sha256: sha256_hex(api_key.as_bytes()),
    };
    let capability_path = symbol_dir.join("capabilities.jsonl");
    let mut capability_store = CapabilityEvidenceStore::open(&capability_path)?;
    let capability_at_ms = wall_clock_ms()?;
    let capability_until_ms = capability_at_ms
        .checked_add(EVIDENCE_TTL_MS)
        .ok_or(BinanceCanaryError::Clock)?;
    capability_store.append_successes(
        &capability_binding,
        capability_at_ms,
        &read_only_probes(&exchange_info, &depth_payload, &second, capability_until_ms)?,
    )?;
    let capabilities = capability_store.current(&capability_binding, capability_at_ms)?;

    // The two signed flat snapshots can take several seconds. Reprice immediately afterward so
    // the deliberate protection entry cannot silently become a stale resting order.
    let entry_depth = public.depth_snapshot(&cfg.symbol, 5)?;
    let entry_book = parse_book(&cfg.symbol, wall_clock_ms()?, &entry_depth)?;
    let (best_bid, best_ask) = best_prices(&entry_book)?;
    let command_price = match (request.phase, request.position_side) {
        (BinanceCanaryPhase::PlaceCancel, PositionSide::Long) => {
            Price::new(best_bid.value() - instrument.price_tick.value())?
        }
        (BinanceCanaryPhase::PlaceCancel, PositionSide::Short) => {
            Price::new(best_ask.value() + instrument.price_tick.value())?
        }
        (BinanceCanaryPhase::Protection, PositionSide::Long) => {
            Price::new(best_ask.value() + instrument.price_tick.value())?
        }
        (BinanceCanaryPhase::Protection, PositionSide::Short) => {
            Price::new(best_bid.value() - instrument.price_tick.value())?
        }
        (_, PositionSide::Net) => return Err(BinanceCanaryError::Request),
    };
    let preflight_now_ms = wall_clock_ms()?;
    let approval = authorize_canary_preflight(CanaryPreflightInput {
        binding: &canary_binding,
        snapshots: &snapshots,
        instrument: &instrument,
        reference_price: command_price,
        now_ms: preflight_now_ms,
        maximum_evidence_age_ms: PREFLIGHT_MAX_AGE_MS,
    })?;

    let usdt: Asset = "USDT".parse()?;
    let binding_valid_until_ms = preflight_now_ms
        .checked_add(60_000)
        .ok_or(BinanceCanaryError::Clock)?;
    let evidence_binding = CanaryEvidenceBinding {
        canary_id: run_id.clone(),
        exchange: owner.exchange.clone(),
        account: owner.account.clone(),
        symbol: cfg.symbol.clone(),
        owner_scope: owner_scope.clone(),
        release_id: canary_binding.release_id.clone(),
        position_side: request.position_side,
        quote_cap: Amount::new(usdt.clone(), Decimal::new(10, 0)),
        risk_cap: Amount::new(usdt, Decimal::new(10, 0)),
        valid_until_ms: binding_valid_until_ms,
    };
    let evidence_path = run_dir.join("evidence.jsonl");
    let mut evidence = CanaryEvidenceJournal::create_new(
        &evidence_path,
        evidence_binding.clone(),
        preflight_now_ms,
    )?;
    let writer_scope = WriterScope {
        exchange: evidence_binding.exchange.clone(),
        account: evidence_binding.account.clone(),
        symbol: evidence_binding.symbol.clone(),
        owner_scope,
    };
    let authority =
        WriterLeaseAuthority::open(symbol_dir.join("writer.json"), writer_scope.clone())?;
    let mut writer = authority.register_initial(preflight_now_ms, PRIVATE_GENERATION)?;
    let run_binding = CanaryRunBinding {
        canary_id: run_id.clone(),
        exchange: evidence_binding.exchange.clone(),
        account: evidence_binding.account.clone(),
        symbol: evidence_binding.symbol.clone(),
        owner_scope: evidence_binding.owner_scope.clone(),
        release_id: evidence_binding.release_id.clone(),
        position_side: evidence_binding.position_side,
        writer_generation: writer.generation,
        readback_generation: writer.readback_generation,
        valid_until_ms: binding_valid_until_ms,
    };
    let mut run =
        CanaryRunState::create_new(run_dir.join("run.json"), run_binding, preflight_now_ms)?;
    let mut commands = CommandJournal::open(run_dir.join("commands.jsonl"))?;
    let command = OrderCommand {
        command_id: CommandId::new(format!("cmd_{started_at_ms}"))?,
        client_order_id: CommandId::new(format!("vcn_{started_at_ms}"))?,
        owner: owner.clone(),
        side: entry_side(request.position_side)?,
        position_side: request.position_side,
        quantity: approval.quantity,
        limit_price: command_price,
        reduce_only: false,
    };
    let _bnb_sequence_permit = super::canary_sequence_runtime::authorize_bnb(
        &request.artifacts_root,
        &cfg.trading_account_id,
        &cfg.symbol,
        &command,
    )?;
    let permit_now_ms = wall_clock_ms()?;
    let permit_kind = match request.phase {
        BinanceCanaryPhase::PlaceCancel => ProbeKind::PostOnlyPlaceCancel,
        BinanceCanaryPhase::Protection => ProbeKind::ProtectionEntry,
    };
    let permit = authorize_probe_permit(ProbePermitInput {
        kind: permit_kind,
        now_ms: permit_now_ms,
        probe_ttl_ms: 3_000,
        binding: &evidence_binding,
        preflight: &approval,
        writer: &writer,
        command: &command,
        execution: ProbeExecutionState {
            command_wal_clean: !commands.has_unresolved(),
            reconciliation_clean: true,
            reconciliation_generation: PRIVATE_GENERATION,
            reconciliation_valid_until_ms: binding_valid_until_ms,
        },
        capabilities: &capabilities,
    })?;
    let submitted_at_ms = wall_clock_ms()?;
    let dispatch = authority.dispatch_guard(&writer, submitted_at_ms)?;
    let receipt_result = match request.phase {
        BinanceCanaryPhase::PlaceCancel => submit_post_only_probe(
            &mut commands,
            &private,
            command.clone(),
            PostOnlyProbePreflight {
                permit,
                writer: &writer,
                run: &mut run,
                now_ms: submitted_at_ms,
                dispatch: &dispatch,
            },
        ),
        BinanceCanaryPhase::Protection => submit_protection_probe_entry(
            &mut commands,
            &private,
            command.clone(),
            PostOnlyProbePreflight {
                permit,
                writer: &writer,
                run: &mut run,
                now_ms: submitted_at_ms,
                dispatch: &dispatch,
            },
        ),
    };
    let receipt = match receipt_result {
        Ok(receipt) => receipt,
        Err(source)
            if matches!(
                commands
                    .receipt(&command.command_id)
                    .map(|receipt| &receipt.state),
                Some(crate::execution::CommandState::Unknown { .. })
            ) =>
        {
            evidence.append_stage(
                "entry_submit_unknown",
                wall_clock_ms()?,
                stage(&[("failure", sha256_hex(source.to_string()))]),
            )?;
            ExecutionReceipt::ProbeAccepted {
                order: recover_unknown_entry(
                    &private,
                    &mut commands,
                    &mut facts,
                    &mut reconciler,
                    &command,
                )?,
            }
        }
        Err(source) => return Err(source.into()),
    };
    drop(dispatch);
    let placed_order = probe_order(receipt)?;
    evidence.append_stage(
        "entry_response",
        wall_clock_ms()?,
        stage(&[
            ("order", sha256_json(&placed_order)?),
            ("command", sha256_json(&command)?),
        ]),
    )?;

    match request.phase {
        BinanceCanaryPhase::PlaceCancel => {
            let _ = cancel_probe(
                &private,
                &authority,
                &writer,
                &mut commands,
                &command,
                started_at_ms,
            )?;
        }
        BinanceCanaryPhase::Protection => {}
    }

    let immediate = if request.phase == BinanceCanaryPhase::Protection {
        position_from_order(&placed_order)?
            .map_or(EntryObservation::Pending, EntryObservation::Position)
    } else {
        EntryObservation::Pending
    };
    let mut observation = match immediate {
        EntryObservation::Position(position) => EntryObservation::Position(position),
        EntryObservation::Terminal(readback) => EntryObservation::Terminal(readback),
        EntryObservation::Pending => wait_for_position_or_terminal(
            &private,
            &cfg.symbol,
            request.position_side,
            &command,
            &mut facts,
            &mut reconciler,
            submitted_at_ms,
        )?,
    };
    if matches!(observation, EntryObservation::Pending) {
        writer = authority.renew(&writer, wall_clock_ms()?)?;
        observation = match cancel_probe(
            &private,
            &authority,
            &writer,
            &mut commands,
            &command,
            started_at_ms,
        )? {
            Some(position) => EntryObservation::Position(position),
            None => wait_for_position_or_terminal(
                &private,
                &cfg.symbol,
                request.position_side,
                &command,
                &mut facts,
                &mut reconciler,
                wall_clock_ms()?,
            )?,
        };
    }
    let (position, readback) = match observation {
        EntryObservation::Position(position) => (Some(position), None),
        EntryObservation::Terminal(readback) => (None, Some(readback)),
        EntryObservation::Pending => return Err(BinanceCanaryError::EntryStillOpen),
    };
    let (terminal_readback, protection_confirmed) = if let Some(position) = position {
        let fill_hash = sha256_json(&position)?;
        let detected_at_ms = wall_clock_ms()?;
        let deadline_ms = match run.filled_unprotected(fill_hash.clone(), detected_at_ms) {
            Ok(deadline_ms) => deadline_ms,
            Err(crate::execution::CanaryRunStateError::Frozen) => submitted_at_ms
                .checked_add(crate::execution::MAX_UNPROTECTED_MS)
                .ok_or(BinanceCanaryError::Clock)?,
            Err(source) => return Err(source.into()),
        };
        evidence.append_stage(
            "fill_detected",
            wall_clock_ms()?,
            stage(&[("position", fill_hash)]),
        )?;
        protect_flatten_and_cancel(
            &private,
            &public,
            &instrument,
            &authority,
            &mut writer,
            &evidence_binding,
            &mut commands,
            &mut run,
            &mut evidence,
            &mut facts,
            &mut reconciler,
            &position,
            deadline_ms,
            submitted_at_ms,
            started_at_ms,
        )?
    } else {
        let readback = readback.ok_or(BinanceCanaryError::Outcome)?;
        ensure_flat(&readback, &cfg.symbol)?;
        let flat_hash = private_readback_hash(&readback)?;
        run.flat(flat_hash.clone(), wall_clock_ms()?)?;
        evidence.seal_terminal(
            wall_clock_ms()?,
            CanaryTerminalState::Flat {
                exact_readback_sha256: flat_hash,
            },
        )?;
        (readback, false)
    };
    ensure_no_open_algos(&private, &cfg.symbol)?;
    let completed_at_ms = wall_clock_ms()?;
    let flat_summary = private_readback_hash(&terminal_readback)?;
    authority.retire_flat(&FlatReceipt {
        receipt_id: format!("flat_{run_id}"),
        predecessor: writer,
        scope: writer_scope,
        readback_generation: PRIVATE_GENERATION
            .checked_add(1)
            .ok_or(BinanceCanaryError::Clock)?,
        summary_sha256: flat_summary.clone(),
    })?;
    // A failed custody proof never earns a capability or the SOL→BNB promotion receipt, but its
    // independently proven Flat terminal state must still retire this exact writer. Otherwise a
    // harmless fail-closed result would permanently prevent its own recovery or later Canary.
    if request.phase == BinanceCanaryPhase::Protection && !protection_confirmed {
        return Err(BinanceCanaryError::ProtectionUnconfirmed);
    }

    let mutation_probes = match request.phase {
        BinanceCanaryPhase::PlaceCancel => vec![
            capability_probe(
                Capability::PlaceLimit,
                "gtx_place_exact_v1",
                &placed_order,
                completed_at_ms,
            )?,
            capability_probe(
                Capability::Cancel,
                "exact_owner_cancel_v1",
                &private_readback_hash(&terminal_readback)?,
                completed_at_ms,
            )?,
            capability_probe(
                Capability::Reconciliation,
                "exact_flat_readback_v1",
                &private_readback_hash(&terminal_readback)?,
                completed_at_ms,
            )?,
        ],
        BinanceCanaryPhase::Protection => vec![
            capability_probe(
                Capability::ReduceOnly,
                "hedge_side_full_reduce_v1",
                &private_readback_hash(&terminal_readback)?,
                completed_at_ms,
            )?,
            capability_probe(
                Capability::Reconciliation,
                "protected_then_flat_readback_v1",
                &private_readback_hash(&terminal_readback)?,
                completed_at_ms,
            )?,
        ],
    };
    capability_store.append_successes(&capability_binding, completed_at_ms, &mutation_probes)?;
    if request.phase == BinanceCanaryPhase::Protection {
        super::canary_sequence_runtime::complete_sol(
            &request.artifacts_root,
            &cfg.trading_account_id,
            &cfg.symbol,
            &evidence,
        )?;
    }

    Ok(BinanceCanaryReport {
        phase: request.phase,
        symbol: cfg.symbol.clone(),
        quantity: approval.quantity,
        entry_notional: approval.notional,
        terminal_flat: true,
        evidence_path,
    })
}

fn ensure_no_unfinished_runs(
    symbol_dir: &std::path::Path,
    now_ms: u64,
) -> Result<(), BinanceCanaryError> {
    for entry in fs::read_dir(symbol_dir).map_err(BinanceCanaryError::Io)? {
        let entry = entry.map_err(BinanceCanaryError::Io)?;
        if !entry.file_type().map_err(BinanceCanaryError::Io)?.is_dir() {
            continue;
        }
        let run_path = entry.path().join("run.json");
        if !run_path.exists() {
            continue;
        }
        let run = CanaryRunState::recover_existing(&run_path, now_ms)?;
        if !run.is_terminal() {
            return Err(BinanceCanaryError::PendingRecovery(run_path));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn protect_flatten_and_cancel(
    private: &PrivateRest,
    public: &PublicRest,
    instrument: &Instrument,
    authority: &WriterLeaseAuthority,
    writer: &mut crate::execution::WriterSession,
    binding: &CanaryEvidenceBinding,
    commands: &mut CommandJournal,
    run: &mut CanaryRunState,
    evidence: &mut CanaryEvidenceJournal,
    facts: &mut Journal,
    reconciler: &mut Reconciler,
    position: &Position,
    deadline_ms: u64,
    submitted_at_ms: u64,
    identity_ms: u64,
) -> Result<(PrivateReadback, bool), BinanceCanaryError> {
    let book_payload = public.depth_snapshot(&binding.symbol, 5)?;
    let book = parse_book(&binding.symbol, wall_clock_ms()?, &book_payload)?;
    let (best_bid, best_ask) = best_prices(&book)?;
    let stop_price = stop_price(position.side, best_bid, best_ask, instrument.price_tick)?;
    let protect_owner = OrderOwner {
        strategy_instance_id: "manual_canary".to_owned(),
        run_id: format!("protection_{identity_ms}"),
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        purpose: OrderPurpose::Protection,
    };
    let stop = StopMarketFullPositionCommand {
        command_id: CommandId::new(format!("stop_{identity_ms}"))?,
        client_algo_id: CommandId::new(format!("vca_{identity_ms}"))?,
        owner: protect_owner.clone(),
        side: close_side(position.side)?,
        position_side: position.side,
        quantity: position.quantity,
        trigger_price: stop_price,
        position_generation: PRIVATE_GENERATION,
    };
    let protect_at_ms = wall_clock_ms()?;
    let mut stop_may_exist = false;
    let mut protection_confirmed = false;
    let mut flatten_position = position.clone();
    if protect_at_ms < deadline_ms && run.require_unprotected_before(protect_at_ms).is_ok() {
        let protection_result = (|| -> Result<(), BinanceCanaryError> {
            let guard = authority.dispatch_guard(writer, protect_at_ms)?;
            stop_may_exist = true;
            let result = submit_stop_market_full_position(
                commands,
                private,
                stop.clone(),
                ProtectionPreflight {
                    instrument,
                    position,
                    private_generation: PRIVATE_GENERATION,
                    position_generation: PRIVATE_GENERATION,
                    account_can_trade: true,
                    hedge_position: true,
                    mark_price_fresh: true,
                },
            );
            drop(guard);
            if !matches!(result?, ExecutionReceipt::ProtectedAlgo { .. }) {
                return Err(BinanceCanaryError::Outcome);
            }
            let installed_at_ms = wall_clock_ms()?;
            run.protection_submitted(sha256_json(&stop)?, installed_at_ms)?;
            flatten_position = private
                .position_readback(&binding.symbol)?
                .into_iter()
                .find(|candidate| candidate.side == position.side && !candidate.quantity.is_zero())
                .ok_or(BinanceCanaryError::NotFlat)?;
            renew_writer_if_needed(authority, writer, wall_clock_ms()?)?;
            let payload = await_algo_visibility(private, &stop, deadline_ms)?;
            let strategy = binance_private::parse_algo_order(
                &payload,
                &binding.symbol,
                stop.client_algo_id.as_str(),
            )?;
            let custody = prove_algo_protection_custody(AlgoProtectionCustodyInput {
                command: &stop,
                position: &flatten_position,
                algo: &strategy,
                writer,
                evidence: ProtectionEvidence {
                    private_generation: PRIVATE_GENERATION,
                    readback_generation: PRIVATE_GENERATION,
                    valid_until_ms: binding.valid_until_ms,
                    observed_at_ms: wall_clock_ms()?,
                },
                writer_role: CustodyWriterRole {
                    predecessor_protected: false,
                    protection_only: false,
                },
                now_ms: wall_clock_ms()?,
            })?;
            run.protected(custody.content_sha256.clone(), wall_clock_ms()?)?;
            evidence.append_stage(
                "protection_custody",
                wall_clock_ms()?,
                stage(&[("custody", custody.content_sha256)]),
            )?;
            protection_confirmed = true;
            Ok(())
        })();
        if let Err(source) = protection_result {
            evidence.append_stage(
                "protection_unconfirmed",
                wall_clock_ms()?,
                stage(&[("failure", sha256_hex(source.to_string()))]),
            )?;
        }
    }

    let renew_at_ms = wall_clock_ms()?;
    *writer = authority.renew(writer, renew_at_ms)?;

    let fresh_depth = public.depth_snapshot(&binding.symbol, 5)?;
    let fresh_book = parse_book(&binding.symbol, wall_clock_ms()?, &fresh_depth)?;
    let (bid, ask) = best_prices(&fresh_book)?;
    let flatten_price =
        aggressive_close_price(flatten_position.side, bid, ask, instrument.price_tick)?;
    let flatten_owner = OrderOwner {
        strategy_instance_id: "manual_canary".to_owned(),
        run_id: format!("flatten_{identity_ms}"),
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        purpose: OrderPurpose::Reduce,
    };
    let flatten_command_id = CommandId::new(format!("flat_{identity_ms}"))?;
    let flatten_client_id = CommandId::new(format!("vcf_{identity_ms}"))?;
    let flatten_at_ms = wall_clock_ms()?;
    let authorization = authorize_emergency_flatten(EmergencyFlattenInput {
        binding,
        authoritative_position: &flatten_position,
        writer,
        dispatch: EmergencyDispatchState {
            now_ms: flatten_at_ms,
            private_generation: PRIVATE_GENERATION,
            readback_generation: PRIVATE_GENERATION,
            position_generation: PRIVATE_GENERATION,
            private_readback_valid_until_ms: binding.valid_until_ms,
            reconciliation_clean: true,
            reconciliation_valid_until_ms: binding.valid_until_ms,
            entry_or_reduce_wal_clean: !commands.has_unresolved_entry_or_reduce(),
            filled_at_ms: submitted_at_ms,
            unprotected_deadline_ms: deadline_ms,
            dispatch_writer_generation: writer.generation,
            dispatch_writer_revision: writer.revision,
        },
        instrument,
        market_price: flatten_price,
        market_price_valid_until_ms: flatten_at_ms
            .checked_add(500)
            .ok_or(BinanceCanaryError::Clock)?,
        risk: &EmergencyRiskEnvelope {
            quote_cap: binding.quote_cap.clone(),
            risk_cap: binding.risk_cap.clone(),
            valid_until_ms: binding.valid_until_ms,
        },
        command_id: &flatten_command_id,
        client_order_id: &flatten_client_id,
        owner: &flatten_owner,
    })?;
    let flatten_hash = sha256_json(&authorization.command)?;
    run.emergency_flattening(flatten_hash.clone(), flatten_at_ms)?;
    evidence.append_stage(
        "flatten_submitted",
        flatten_at_ms,
        stage(&[("command", flatten_hash)]),
    )?;
    let guard = authority.dispatch_guard(writer, flatten_at_ms)?;
    let _ = submit_emergency_flatten(
        commands,
        private,
        authorization,
        instrument,
        &flatten_position,
        flatten_at_ms,
        &guard,
    )?;
    drop(guard);

    let flat_readback = wait_until_flat(
        private,
        &binding.symbol,
        flatten_position.side,
        authority,
        writer,
    )?;
    if stop_may_exist {
        let renew_at_ms = wall_clock_ms()?;
        *writer = authority.renew(writer, renew_at_ms)?;
        settle_algo(
            private,
            authority,
            writer,
            commands,
            facts,
            reconciler,
            &stop,
            identity_ms,
        )?;
    }
    let flat_hash = private_readback_hash(&flat_readback)?;
    run.flat(flat_hash.clone(), wall_clock_ms()?)?;
    evidence.seal_terminal(
        wall_clock_ms()?,
        CanaryTerminalState::Flat {
            exact_readback_sha256: flat_hash,
        },
    )?;
    Ok((flat_readback, protection_confirmed))
}

fn cancel_probe(
    private: &PrivateRest,
    authority: &WriterLeaseAuthority,
    writer: &crate::execution::WriterSession,
    commands: &mut CommandJournal,
    entry: &OrderCommand,
    identity_ms: u64,
) -> Result<Option<Position>, BinanceCanaryError> {
    let now_ms = wall_clock_ms()?;
    let guard = authority.dispatch_guard(writer, now_ms)?;
    let receipt = submit_cancel(
        commands,
        private,
        CancelCommand {
            command_id: CommandId::new(format!("cancel_{identity_ms}"))?,
            owner: entry.owner.clone(),
            target_client_order_id: entry.client_order_id.clone(),
        },
    )?;
    drop(guard);
    match receipt {
        ExecutionReceipt::Cancelled { order } | ExecutionReceipt::CancelNotApplied { order } => {
            position_from_order(&order)
        }
        _ => Err(BinanceCanaryError::Outcome),
    }
}

#[allow(clippy::too_many_arguments)]
fn settle_algo(
    private: &PrivateRest,
    authority: &WriterLeaseAuthority,
    writer: &mut crate::execution::WriterSession,
    commands: &mut CommandJournal,
    facts: &mut Journal,
    reconciler: &mut Reconciler,
    stop: &StopMarketFullPositionCommand,
    identity_ms: u64,
) -> Result<(), BinanceCanaryError> {
    renew_writer_if_needed(authority, writer, wall_clock_ms()?)?;
    match private.algo_order_by_client_algo_id(stop.client_algo_id.as_str()) {
        Ok(payload) => {
            let current = binance_private::parse_algo_order(
                &payload,
                &stop.owner.symbol,
                stop.client_algo_id.as_str(),
            )?;
            if current.status == ConditionalStrategyStatus::Current {
                renew_writer_if_needed(authority, writer, wall_clock_ms()?)?;
                cancel_algo(
                    private,
                    authority,
                    writer,
                    commands,
                    facts,
                    reconciler,
                    stop,
                    identity_ms,
                )
            } else if matches!(
                current.status,
                ConditionalStrategyStatus::Cancelled
                    | ConditionalStrategyStatus::NonCancelledTerminal
                    | ConditionalStrategyStatus::Rejected
            ) {
                Ok(())
            } else {
                Err(BinanceCanaryError::ProtectionStillActive)
            }
        }
        Err(PrivateError::Rejected { .. }) => {
            match private.algo_order_history(&stop.owner.symbol) {
                Ok(payload) => {
                    let history = binance_private::parse_algo_order(
                        &payload,
                        &stop.owner.symbol,
                        stop.client_algo_id.as_str(),
                    )?;
                    if matches!(
                        history.status,
                        ConditionalStrategyStatus::Cancelled
                            | ConditionalStrategyStatus::NonCancelledTerminal
                            | ConditionalStrategyStatus::Rejected
                    ) {
                        Ok(())
                    } else {
                        Err(BinanceCanaryError::ProtectionStillActive)
                    }
                }
                Err(PrivateError::Rejected { .. }) => {
                    Err(BinanceCanaryError::ProtectionStillActive)
                }
                Err(source) => Err(source.into()),
            }
        }
        Err(source) => Err(source.into()),
    }
}

#[allow(clippy::too_many_arguments)]
fn cancel_algo(
    private: &PrivateRest,
    authority: &WriterLeaseAuthority,
    writer: &crate::execution::WriterSession,
    commands: &mut CommandJournal,
    facts: &mut Journal,
    reconciler: &mut Reconciler,
    stop: &StopMarketFullPositionCommand,
    identity_ms: u64,
) -> Result<(), BinanceCanaryError> {
    let now_ms = wall_clock_ms()?;
    let guard = authority.dispatch_guard(writer, now_ms)?;
    let cancel_id = CommandId::new(format!("stop_cancel_{identity_ms}"))?;
    let receipt = submit_cancel(
        commands,
        private,
        CancelCommand {
            command_id: cancel_id.clone(),
            owner: stop.owner.clone(),
            target_client_order_id: stop.client_algo_id.clone(),
        },
    )?;
    drop(guard);
    if !matches!(receipt, ExecutionReceipt::CancelAlgoPendingReadback) {
        return Err(BinanceCanaryError::Outcome);
    }
    for _ in 0..40 {
        if resolve_unknown_order_by_readback(
            commands,
            private,
            facts,
            reconciler,
            &cancel_id,
            PRIVATE_GENERATION,
            wall_clock_ms()?,
        )? {
            return match commands.receipt(&cancel_id).map(|item| &item.state) {
                Some(crate::execution::CommandState::Accepted { .. }) => Ok(()),
                _ => Err(BinanceCanaryError::ProtectionStillActive),
            };
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err(BinanceCanaryError::ProtectionStillActive)
}

enum EntryObservation {
    Position(Position),
    Terminal(PrivateReadback),
    Pending,
}

fn recover_unknown_entry(
    private: &PrivateRest,
    commands: &mut CommandJournal,
    facts: &mut Journal,
    reconciler: &mut Reconciler,
    command: &OrderCommand,
) -> Result<Order, BinanceCanaryError> {
    for _ in 0..10 {
        match resolve_unknown_order_by_readback(
            commands,
            private,
            facts,
            reconciler,
            &command.command_id,
            PRIVATE_GENERATION,
            wall_clock_ms()?,
        ) {
            Ok(true) => {
                let payload = private
                    .order_by_client_id(&command.owner.symbol, command.client_order_id.as_str())?;
                return Ok(binance_private::parse_order(
                    &payload,
                    &command.owner.symbol,
                )?);
            }
            Ok(false) | Err(ExecutionError::Venue(PrivateError::Rejected { .. })) => {
                thread::sleep(Duration::from_millis(100));
            }
            Err(source) => return Err(source.into()),
        }
    }
    Err(BinanceCanaryError::EntryUnknown)
}

fn wait_for_position_or_terminal(
    private: &PrivateRest,
    symbol: &crate::domain::Symbol,
    side: PositionSide,
    command: &OrderCommand,
    facts: &mut Journal,
    reconciler: &mut Reconciler,
    submitted_at_ms: u64,
) -> Result<EntryObservation, BinanceCanaryError> {
    for _ in 0..20 {
        let positions = private.position_readback(symbol)?;
        let observed_at_ms = wall_clock_ms()?;
        reconciler.accept_readback(
            facts,
            ReadbackBatch {
                generation: PRIVATE_GENERATION,
                received_at_ms: observed_at_ms,
                balances: &[],
                positions: &positions,
                orders: &[],
                fills: &[],
            },
        )?;
        if let Some(position) = positions
            .into_iter()
            .find(|position| position.side == side && !position.quantity.is_zero())
        {
            return Ok(EntryObservation::Position(position));
        }
        let payload = private.order_by_client_id(symbol, command.client_order_id.as_str())?;
        let order = binance_private::parse_order(&payload, symbol)?;
        if matches!(
            order.state,
            OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
        ) && order.filled_quantity.is_zero()
        {
            return Ok(EntryObservation::Terminal(private.readback(symbol)?));
        }
        if let Some(position) = position_from_order(&order)? {
            return Ok(EntryObservation::Position(position));
        }
        if observed_at_ms.saturating_sub(submitted_at_ms) >= 1_000 {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(EntryObservation::Pending)
}

fn wait_until_flat(
    private: &PrivateRest,
    symbol: &crate::domain::Symbol,
    side: PositionSide,
    authority: &WriterLeaseAuthority,
    writer: &mut crate::execution::WriterSession,
) -> Result<PrivateReadback, BinanceCanaryError> {
    for _ in 0..40 {
        renew_writer_if_needed(authority, writer, wall_clock_ms()?)?;
        let positions = private.position_readback(symbol)?;
        let leg = positions.iter().find(|position| position.side == side);
        if leg.is_some_and(|position| position.quantity.is_zero()) {
            let readback = private.readback(symbol)?;
            ensure_flat(&readback, symbol)?;
            return Ok(readback);
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err(BinanceCanaryError::NotFlat)
}

fn renew_writer_if_needed(
    authority: &WriterLeaseAuthority,
    writer: &mut crate::execution::WriterSession,
    now_ms: u64,
) -> Result<(), BinanceCanaryError> {
    if writer.valid_until_ms.saturating_sub(now_ms) <= WRITER_LEASE_TTL_MS / 2 {
        *writer = authority.renew(writer, now_ms)?;
    }
    Ok(())
}

fn canary_snapshot(
    binding: &CanaryBinding,
    readback: &PrivateReadback,
    observed_at_ms: u64,
    instrument: &Instrument,
) -> Result<CanarySnapshot, BinanceCanaryError> {
    let available = readback
        .balances
        .iter()
        .find(|balance| balance.asset.as_str() == "USDT")
        .ok_or(BinanceCanaryError::Outcome)?;
    Ok(CanarySnapshot {
        binding: binding.clone(),
        observed_at_ms,
        generation: PRIVATE_GENERATION,
        instrument_generation: FieldState::Known(instrument.generation),
        can_trade: FieldState::Known(readback.capabilities.can_trade),
        hedge_mode: FieldState::Known(readback.capabilities.hedge_position),
        positions: readback
            .positions
            .iter()
            .map(|position| CanaryPosition {
                side: FieldState::Known(position.side),
                quantity: FieldState::Known(position.quantity),
            })
            .collect(),
        open_orders: FieldState::Known(
            u32::try_from(readback.orders.len()).map_err(|_| BinanceCanaryError::Outcome)?,
        ),
        available_margin: FieldState::Known(Amount::new(
            available.asset.clone(),
            available.available_balance,
        )),
        owner_conflict: FieldState::Known(false),
        execution_unknown: FieldState::Known(false),
    })
}

fn accept_readback(
    facts: &mut Journal,
    reconciler: &mut Reconciler,
    readback: &PrivateReadback,
    received_at_ms: u64,
) -> Result<(), BinanceCanaryError> {
    reconciler.accept_readback(
        facts,
        ReadbackBatch {
            generation: PRIVATE_GENERATION,
            received_at_ms,
            balances: &readback.balances,
            positions: &readback.positions,
            orders: &readback.orders,
            fills: &readback.fills,
        },
    )?;
    Ok(())
}

fn ensure_flat(
    readback: &PrivateReadback,
    symbol: &crate::domain::Symbol,
) -> Result<(), BinanceCanaryError> {
    if readback.positions.len() != 2
        || readback
            .positions
            .iter()
            .any(|position| &position.symbol != symbol || !position.quantity.is_zero())
        || !readback.orders.is_empty()
    {
        return Err(BinanceCanaryError::NotFlat);
    }
    Ok(())
}

fn ensure_no_open_algos(
    private: &PrivateRest,
    symbol: &crate::domain::Symbol,
) -> Result<(), BinanceCanaryError> {
    let payload = private.open_algo_orders(symbol)?;
    let value: serde_json::Value = serde_json::from_str(&payload)?;
    if value.as_array().is_some_and(Vec::is_empty) {
        Ok(())
    } else {
        Err(BinanceCanaryError::ProtectionStillActive)
    }
}

/// A successful Algo placement response may precede the exact query endpoint by a few network
/// or matching-engine ticks. This retries only a signed readback that reports that the just-owned
/// identity is not visible yet; it never retries the mutation and never extends the unprotected
/// deadline.
fn await_algo_visibility(
    private: &PrivateRest,
    command: &StopMarketFullPositionCommand,
    deadline_ms: u64,
) -> Result<String, BinanceCanaryError> {
    loop {
        let now_ms = wall_clock_ms()?;
        if now_ms >= deadline_ms {
            return Err(BinanceCanaryError::ProtectionUnconfirmed);
        }
        match private.algo_order_by_client_algo_id(command.client_algo_id.as_str()) {
            Ok(payload) => return Ok(payload),
            Err(error) if retryable_algo_visibility_error(&error) => {
                let remaining_ms = deadline_ms.saturating_sub(now_ms);
                let sleep_ms = remaining_ms.min(ALGO_VISIBILITY_RETRY_MS);
                if sleep_ms == 0 {
                    return Err(BinanceCanaryError::ProtectionUnconfirmed);
                }
                thread::sleep(Duration::from_millis(sleep_ms));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn retryable_algo_visibility_error(error: &PrivateError) -> bool {
    matches!(
        error,
        PrivateError::Rejected {
            api_code: Some(-2013),
            ..
        } | PrivateError::Unknown(_)
            | PrivateError::Http
    )
}

fn read_only_probes(
    exchange_info: &str,
    depth: &str,
    readback: &PrivateReadback,
    valid_until_ms: u64,
) -> Result<Vec<CapabilityProbe>, BinanceCanaryError> {
    Ok(vec![
        CapabilityProbe::new(
            Capability::InstrumentRules,
            "binance_fapi_exchange_info_instrument_v1",
            sha256_hex(exchange_info),
            valid_until_ms,
        )?,
        CapabilityProbe::new(
            Capability::PublicMarket,
            "binance_fapi_depth_snapshot_v1",
            sha256_hex(depth),
            valid_until_ms,
        )?,
        CapabilityProbe::new(
            Capability::PrivateReadback,
            "binance_papi_um_signed_readback_v1",
            private_readback_hash(readback)?,
            valid_until_ms,
        )?,
        CapabilityProbe::new(
            Capability::PrivateStream,
            "binance_papi_listen_key_connect_close_v1",
            sha256_hex("connected_and_closed"),
            valid_until_ms,
        )?,
    ])
}

fn capability_probe<T: serde::Serialize>(
    capability: Capability,
    name: &str,
    value: &T,
    verified_at_ms: u64,
) -> Result<CapabilityProbe, BinanceCanaryError> {
    CapabilityProbe::new(
        capability,
        name,
        sha256_json(value)?,
        verified_at_ms
            .checked_add(EVIDENCE_TTL_MS)
            .ok_or(BinanceCanaryError::Clock)?,
    )
    .map_err(Into::into)
}

fn probe_order(receipt: ExecutionReceipt) -> Result<Order, BinanceCanaryError> {
    match receipt {
        ExecutionReceipt::ProbeAccepted { order } => Ok(order),
        _ => Err(BinanceCanaryError::Outcome),
    }
}

fn position_from_order(order: &Order) -> Result<Option<Position>, BinanceCanaryError> {
    if order.filled_quantity.is_zero() {
        return Ok(None);
    }
    let side = match &order.position_side {
        FieldState::Known(side @ (PositionSide::Long | PositionSide::Short)) => *side,
        _ => return Err(BinanceCanaryError::Outcome),
    };
    let entry_price = match &order.average_price {
        FieldState::Known(price) => Some(*price),
        _ => order.limit_price,
    };
    Ok(Some(Position {
        symbol: order.symbol.clone(),
        side,
        quantity: order.filled_quantity,
        entry_price,
        mark_price: entry_price,
    }))
}

fn private_readback_hash(readback: &PrivateReadback) -> Result<String, BinanceCanaryError> {
    sha256_json(&serde_json::json!({
        "can_trade": readback.capabilities.can_trade,
        "one_way": readback.capabilities.one_way_position,
        "hedge": readback.capabilities.hedge_position,
        "balances": readback.balances,
        "positions": readback.positions,
        "orders": readback.orders,
        "fills": readback.fills,
    }))
}

fn parse_book(
    symbol: &crate::domain::Symbol,
    received_at_ms: u64,
    payload: &str,
) -> Result<crate::domain::MarketSnapshot, BinanceCanaryError> {
    let record = RawMarketRecord::new(
        RawSource::RestSnapshot,
        symbol.clone(),
        PRIVATE_GENERATION,
        received_at_ms,
        payload.to_owned(),
    )?;
    match normalize(&record, &native_symbol(symbol))? {
        MarketEvent::Snapshot(snapshot) => Ok(snapshot),
        _ => Err(BinanceCanaryError::Outcome),
    }
}

fn best_prices(book: &crate::domain::MarketSnapshot) -> Result<(Price, Price), BinanceCanaryError> {
    let bid = book
        .bids
        .iter()
        .max_by_key(|level| level.price)
        .map(|level| level.price)
        .ok_or(BinanceCanaryError::Outcome)?;
    let ask = book
        .asks
        .iter()
        .min_by_key(|level| level.price)
        .map(|level| level.price)
        .ok_or(BinanceCanaryError::Outcome)?;
    if bid >= ask {
        return Err(BinanceCanaryError::Outcome);
    }
    Ok((bid, ask))
}

fn stop_price(
    side: PositionSide,
    bid: Price,
    ask: Price,
    tick: Price,
) -> Result<Price, BinanceCanaryError> {
    let value = match side {
        PositionSide::Long => align_down(bid.value() * Decimal::new(98, 2), tick.value()),
        PositionSide::Short => align_up(ask.value() * Decimal::new(102, 2), tick.value()),
        PositionSide::Net => return Err(BinanceCanaryError::Request),
    };
    Price::new(value).map_err(Into::into)
}

fn aggressive_close_price(
    side: PositionSide,
    bid: Price,
    ask: Price,
    tick: Price,
) -> Result<Price, BinanceCanaryError> {
    let value = match side {
        PositionSide::Long => align_down(bid.value() * Decimal::new(99, 2), tick.value()),
        PositionSide::Short => align_up(ask.value() * Decimal::new(101, 2), tick.value()),
        PositionSide::Net => return Err(BinanceCanaryError::Request),
    };
    Price::new(value).map_err(Into::into)
}

fn align_down(value: Decimal, step: Decimal) -> Decimal {
    value - (value % step)
}

fn align_up(value: Decimal, step: Decimal) -> Decimal {
    let remainder = value % step;
    if remainder.is_zero() {
        value
    } else {
        value + (step - remainder)
    }
}

fn entry_side(side: PositionSide) -> Result<OrderSide, BinanceCanaryError> {
    match side {
        PositionSide::Long => Ok(OrderSide::Buy),
        PositionSide::Short => Ok(OrderSide::Sell),
        PositionSide::Net => Err(BinanceCanaryError::Request),
    }
}

fn close_side(side: PositionSide) -> Result<OrderSide, BinanceCanaryError> {
    match side {
        PositionSide::Long => Ok(OrderSide::Sell),
        PositionSide::Short => Ok(OrderSide::Buy),
        PositionSide::Net => Err(BinanceCanaryError::Request),
    }
}

fn stage(values: &[(&str, String)]) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(key, value)| ((*key).to_owned(), value.clone()))
        .collect()
}

fn sha256_json<T: serde::Serialize>(value: &T) -> Result<String, BinanceCanaryError> {
    Ok(sha256_hex(serde_json::to_vec(value)?))
}

fn verify_private_stream(client: &PrivateRest) -> Result<(), BinanceCanaryError> {
    let listen_key = client.create_user_stream()?;
    let socket = PrivateStreamSocket::connect(&listen_key)?;
    drop(socket);
    // Connectivity checks do not own the account-scoped remote listen key.
    Ok(())
}

fn wall_clock_ms() -> Result<u64, BinanceCanaryError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceCanaryError::Clock)?;
    u64::try_from(elapsed.as_millis()).map_err(|_| BinanceCanaryError::Clock)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_unseen_algo_or_an_unknown_readback_is_visibility_retryable() {
        assert!(retryable_algo_visibility_error(&PrivateError::Rejected {
            status: 400,
            api_code: Some(-2013),
        }));
        assert!(retryable_algo_visibility_error(&PrivateError::Unknown(500)));
        assert!(retryable_algo_visibility_error(&PrivateError::Http));
        assert!(!retryable_algo_visibility_error(&PrivateError::Rejected {
            status: 400,
            api_code: Some(-2015),
        }));
        assert!(!retryable_algo_visibility_error(
            &PrivateError::RateLimited(429)
        ));
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BinanceCanaryError {
    #[error("Canary request must use an absolute artifact root and LONG or SHORT")]
    Request,
    #[error("Canary wall clock is unavailable or overflowed")]
    Clock,
    #[error("Canary exchange outcome was not the exact expected lifecycle")]
    Outcome,
    #[error("Canary entry remained open after exact cancellation/readback")]
    EntryStillOpen,
    #[error("Canary entry UNKNOWN did not resolve by exact signed readback")]
    EntryUnknown,
    #[error("Canary did not converge to an exact flat scope")]
    NotFlat,
    #[error("Canary conditional protection remains active")]
    ProtectionStillActive,
    #[error("Canary flattened safely but did not prove exact protection custody")]
    ProtectionUnconfirmed,
    #[error("Canary has an unfinished durable run requiring recovery: {0}")]
    PendingRecovery(PathBuf),
    #[error("Canary artifact I/O failed: {0}")]
    Io(#[source] std::io::Error),
    #[error(transparent)]
    Public(#[from] crate::exchange::binance::PublicError),
    #[error(transparent)]
    Private(#[from] crate::exchange::binance::PrivateError),
    #[error(transparent)]
    PrivateReadback(#[from] crate::exchange::binance::PrivateReadbackError),
    #[error(transparent)]
    Binance(#[from] crate::exchange::binance::BinanceError),
    #[error(transparent)]
    PrivateParse(#[from] crate::exchange::binance_private::PrivateParseError),
    #[error(transparent)]
    Raw(#[from] crate::market::RawError),
    #[error(transparent)]
    Amount(#[from] crate::domain::AmountError),
    #[error(transparent)]
    Command(#[from] crate::domain::CommandError),
    #[error(transparent)]
    Storage(#[from] crate::storage::StorageError),
    #[error(transparent)]
    Reconciliation(#[from] crate::execution::ReconciliationError),
    #[error(transparent)]
    Capability(#[from] crate::execution::CapabilityEvidenceError),
    #[error(transparent)]
    Preflight(#[from] crate::execution::CanaryPreflightError),
    #[error(transparent)]
    Evidence(#[from] crate::execution::CanaryEvidenceError),
    #[error(transparent)]
    Writer(#[from] crate::execution::WriterLeaseError),
    #[error("Canary account writer registry failed closed: {reason}")]
    WriterRegistry { reason: String },
    #[error(transparent)]
    Run(#[from] crate::execution::CanaryRunStateError),
    #[error(transparent)]
    Probe(#[from] crate::execution::ProbeGateError),
    #[error(transparent)]
    Execution(#[from] crate::execution::ExecutionError),
    #[error(transparent)]
    Custody(#[from] crate::execution::ProtectionCustodyError),
    #[error(transparent)]
    Emergency(#[from] crate::execution::EmergencyFlattenError),
    #[error(transparent)]
    Journal(#[from] crate::execution::CommandJournalError),
    #[error(transparent)]
    SequenceRuntime(#[from] super::canary_sequence_runtime::CanarySequenceRuntimeError),
    #[error(transparent)]
    RecoveryRuntime(#[from] super::canary_recovery_runtime::BinanceCanaryRecoveryError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Asset(#[from] crate::domain::SymbolError),
}
