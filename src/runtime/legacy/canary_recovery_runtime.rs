use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    config::Config,
    domain::{CommandId, FieldState, NativeOrderFamily, PositionSide, Price, Symbol},
    exchange::{
        binance::{
            PrivateCredentials, PrivateRest, PublicRest, native_symbol, parse_depth_best_prices,
            parse_instrument,
        },
        binance_private,
    },
    execution::{
        CanaryRunBinding, CanaryRunPhase, CanaryRunState, CommandJournal, FlatReceipt,
        RecoveryCancelInput, RecoveryObservationProof, RecoveryReduceInput,
        RecoveryWriterAuthority, RecoveryWriterScope, WriterLeaseAuthority, WriterScope,
        authorize_recovery_cancel, authorize_recovery_reduce, resolve_unknown_order_by_readback,
        submit_recovery_cancel, submit_recovery_reduce,
    },
    storage::Journal,
};

use super::{
    canary::MAINNET_CANARY_OWNER_SCOPE,
    canary_recovery::{
        AlgoOrderReadback, CANARY_RECOVERY_SCHEMA_VERSION, CanaryRecoveryCandidate,
        CanaryRecoveryPlan, HedgePositionReadback, OrdinaryOrderReadback, RecoveryAlgoOrder,
        RecoveryOrdinaryOrder, SignedCanaryReadback, plan_canary_recovery,
        plan_terminal_flat_writer_retirement,
    },
};

const RECEIPT_SCHEMA_VERSION: u16 = 1;
const RECEIPT_FILE: &str = "recovery_flat_receipt.json";
const LOCK_FILE: &str = "canary_recovery.lock";
const MAX_MUTATION_ATTEMPTS: usize = 32;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BinanceCanaryRecoveryReport {
    pub symbol: Symbol,
    pub sealed_flat: Vec<PathBuf>,
    pub exact_cancel_required: Vec<PathBuf>,
    pub emergency_flatten_required: Vec<PathBuf>,
    pub remained_fenced: Vec<PathBuf>,
    pub mutation_attempts: usize,
}

/// Performs signed recovery and seals only runs proven clean by two independent account
/// observations. Mutations are opt-in, execute one exact recovery action at a time, and always
/// require a completely new signed observation pair before another action can be authorized.
pub fn run_binance_canary_recovery(
    cfg: &Config,
    artifacts_root: &Path,
    allow_mutations: bool,
) -> Result<BinanceCanaryRecoveryReport, BinanceCanaryRecoveryError> {
    if !artifacts_root.is_absolute() {
        return Err(BinanceCanaryRecoveryError::Path);
    }
    let symbol_dir = artifacts_root.join(native_symbol(&cfg.symbol).to_ascii_lowercase());
    let symbol_dir =
        fs::canonicalize(&symbol_dir).map_err(|source| artifact(&symbol_dir, source))?;
    if !symbol_dir.is_dir() {
        return Err(BinanceCanaryRecoveryError::Path);
    }
    let _account_writer_root = allow_mutations
        .then(|| {
            super::stage7_writer_registry::acquire(
                &WriterScope {
                    exchange: "binance".to_owned(),
                    account: cfg.trading_account_id.clone(),
                    symbol: cfg.symbol.clone(),
                    owner_scope: MAINNET_CANARY_OWNER_SCOPE.to_owned(),
                },
                artifacts_root,
            )
            .map_err(|error| BinanceCanaryRecoveryError::WriterRegistry {
                reason: error.to_string(),
            })
        })
        .transpose()?;
    let _lock = RecoveryLock::acquire(&symbol_dir.join(LOCK_FILE))?;
    let credentials = PrivateCredentials::from_environment()?;
    let private = PrivateRest::production(
        credentials,
        cfg.binance
            .as_ref()
            .ok_or(BinanceCanaryRecoveryError::Path)?
            .account_binding,
    )?;
    let expected_signer = private.recovery_signer_sha256();
    let mut report = BinanceCanaryRecoveryReport {
        symbol: cfg.symbol.clone(),
        sealed_flat: Vec::new(),
        exact_cancel_required: Vec::new(),
        emergency_flatten_required: Vec::new(),
        remained_fenced: Vec::new(),
        mutation_attempts: 0,
    };
    loop {
        let now_ms = wall_clock_ms()?;
        let mut discovered = discover_runs(&symbol_dir, now_ms)?;
        let terminal_debt =
            validate_and_bind_terminal_receipts(&mut discovered, &private, &expected_signer)?;
        settle_terminal_safe_unknown_debt(&mut discovered, &terminal_debt, &private)?;
        for path in
            validate_and_bind_terminal_receipts(&mut discovered, &private, &expected_signer)?
        {
            push_unique(&mut report.remained_fenced, &path);
        }
        for run in &mut discovered {
            if !run.state.is_terminal() && resume_receipt(run, &private, &expected_signer)? {
                push_unique(&mut report.sealed_flat, &run.run_path);
            }
        }
        // A Prepared run can become terminal through `resume_receipt` in this same turn. Retire
        // its exact writer only after that durable state transition, otherwise the following
        // Canary would correctly remain fenced until a second recovery invocation.
        retire_terminal_flat_writers(&symbol_dir, &discovered, &private, &expected_signer)?;
        if discovered.iter().all(|run| run.state.is_terminal()) {
            finalize_report(&mut report);
            return Ok(report);
        }
        let ownership = ownership_index(&discovered)?;
        let first_raw = observe(&private, &cfg.symbol, &ownership)?;
        thread::sleep(Duration::from_millis(2));
        let second_raw = observe(&private, &cfg.symbol, &ownership)?;
        let mut attempted_mutation = false;
        for run in &mut discovered {
            if run.state.is_terminal() {
                continue;
            }
            let candidate = CanaryRecoveryCandidate {
                binding: run.state.binding().clone(),
                phase: run.state.phase().clone(),
                frozen: run.state.is_frozen(),
            };
            let first = sign_readback(&private, &candidate, &first_raw, 1)?;
            let second = sign_readback(&private, &candidate, &second_raw, 2)?;
            match plan_canary_recovery(&candidate, &expected_signer, &first, &second) {
                plan @ CanaryRecoveryPlan::SealFlat { .. } => {
                    if has_unresolved_commands(run)? {
                        push_unique(&mut report.remained_fenced, &run.run_path);
                        continue;
                    }
                    let receipt = persist_receipt(run, first, second, &plan)?;
                    validate_receipt(&receipt, run, &private, &expected_signer)?;
                    run.state.seal_recovered_flat(
                        receipt.receipt_sha256.clone(),
                        receipt.sealed_at_ms,
                    )?;
                    push_unique(&mut report.sealed_flat, &run.run_path);
                }
                plan @ CanaryRecoveryPlan::ExactCancel { .. } => {
                    push_unique(&mut report.exact_cancel_required, &run.run_path);
                    if allow_mutations {
                        attempted_mutation = execute_one_recovery_action(
                            &symbol_dir,
                            run,
                            &plan,
                            &second,
                            &private,
                        )?;
                        if !attempted_mutation {
                            push_unique(&mut report.remained_fenced, &run.run_path);
                        }
                    }
                }
                plan @ CanaryRecoveryPlan::EmergencyFlatten { .. } => {
                    push_unique(&mut report.emergency_flatten_required, &run.run_path);
                    if allow_mutations {
                        attempted_mutation = execute_one_recovery_action(
                            &symbol_dir,
                            run,
                            &plan,
                            &second,
                            &private,
                        )?;
                        if !attempted_mutation {
                            push_unique(&mut report.remained_fenced, &run.run_path);
                        }
                    }
                }
                CanaryRecoveryPlan::RemainFenced { .. } => {
                    push_unique(&mut report.remained_fenced, &run.run_path);
                }
            }
            if attempted_mutation {
                report.mutation_attempts += 1;
                break;
            }
        }
        if !attempted_mutation || !allow_mutations {
            finalize_report(&mut report);
            return Ok(report);
        }
        if report.mutation_attempts >= MAX_MUTATION_ATTEMPTS {
            return Err(BinanceCanaryRecoveryError::MutationLimit);
        }
        // Every side effect, including UNKNOWN, is followed by a completely new pair of signed
        // observations before another action can be authorized.
    }
}

/// A terminal Flat run may have returned an error after it had already reduced risk and sealed
/// its own evidence. Recovery can retire only that exact active writer after a new signed,
/// two-observation Flat proof. This is a local authority cleanup, never a venue mutation.
fn retire_terminal_flat_writers(
    symbol_dir: &Path,
    runs: &[DiscoveredRun],
    private: &PrivateRest,
    expected_signer: &str,
) -> Result<(), BinanceCanaryRecoveryError> {
    let Some(first_run) = runs.first() else {
        return Ok(());
    };
    let ownership = ownership_index(runs)?;
    let first_raw = observe(private, &first_run.state.binding().symbol, &ownership)?;
    thread::sleep(Duration::from_millis(2));
    let second_raw = observe(private, &first_run.state.binding().symbol, &ownership)?;
    for run in runs.iter().filter(|run| run.state.is_terminal()) {
        // The shared root writer belongs only to the fixed Stage 4 manual Canary scope. Frozen
        // migration-era runs use their own historical scopes and must never be opened against it.
        if run.state.binding().owner_scope != MAINNET_CANARY_OWNER_SCOPE {
            continue;
        }
        if has_unresolved_commands(run)? {
            continue;
        }
        let candidate = CanaryRecoveryCandidate {
            binding: run.state.binding().clone(),
            phase: run.state.phase().clone(),
            // This path can only inspect an already terminal Flat run. Expiry must keep an
            // unfinished run fenced, but it cannot invalidate a newer two-observation proof
            // used solely to retire the matching local writer.
            frozen: false,
        };
        let first = sign_readback(private, &candidate, &first_raw, 1)?;
        let second = sign_readback(private, &candidate, &second_raw, 2)?;
        if !matches!(
            plan_terminal_flat_writer_retirement(&candidate, expected_signer, &first, &second),
            CanaryRecoveryPlan::SealFlat { .. }
        ) {
            continue;
        }
        let scope = WriterScope {
            exchange: candidate.binding.exchange.clone(),
            account: candidate.binding.account.clone(),
            symbol: candidate.binding.symbol.clone(),
            owner_scope: candidate.binding.owner_scope.clone(),
        };
        let authority = WriterLeaseAuthority::open(symbol_dir.join("writer.json"), scope.clone())?;
        let Some(active) = authority.active_session()? else {
            continue;
        };
        if active.generation != candidate.binding.writer_generation
            || active.readback_generation != candidate.binding.readback_generation
        {
            continue;
        }
        authority.retire_flat(&FlatReceipt {
            receipt_id: format!("terminal_flat_{}", candidate.binding.canary_id),
            predecessor: active.clone(),
            scope,
            readback_generation: active
                .readback_generation
                .checked_add(1)
                .ok_or(BinanceCanaryRecoveryError::Receipt)?,
            summary_sha256: second.payload_sha256,
        })?;
    }
    Ok(())
}

fn execute_one_recovery_action(
    symbol_dir: &Path,
    run: &mut DiscoveredRun,
    plan: &CanaryRecoveryPlan,
    second: &SignedCanaryReadback,
    private: &PrivateRest,
) -> Result<bool, BinanceCanaryRecoveryError> {
    let now_ms = wall_clock_ms()?;
    let proof = RecoveryObservationProof {
        generation: second.generation,
        observed_at_ms: second.observed_at_ms,
        valid_until_ms: second
            .observed_at_ms
            .checked_add(30_000)
            .ok_or(BinanceCanaryRecoveryError::Clock)?,
        payload_sha256: second.payload_sha256.clone(),
        signature_verified: second.signature_verified
            && private.verify_recovery_payload_signature(
                &second.payload_sha256,
                &second.signature_sha256,
            ),
    };
    let authority = RecoveryWriterAuthority::open(
        symbol_dir.join("writer.json"),
        RecoveryWriterScope {
            exchange: run.state.binding().exchange.clone(),
            account: run.state.binding().account.clone(),
            symbol: run.state.binding().symbol.clone(),
        },
    )?;
    let mut commands = CommandJournal::open(run.run_dir.join("commands.jsonl"))?;
    match plan {
        CanaryRecoveryPlan::ExactCancel {
            ordinary, algos, ..
        } => {
            let (command_id, client_id, family) = if let Some(cancel) = ordinary.first() {
                (
                    cancel.command_id.as_str(),
                    cancel.client_order_id.as_str(),
                    NativeOrderFamily::UmOrder,
                )
            } else if let Some(cancel) = algos.first() {
                (
                    cancel.command_id.as_str(),
                    cancel.client_algo_id.as_str(),
                    NativeOrderFamily::UmAlgo,
                )
            } else {
                return Ok(false);
            };
            let target = CommandId::new(client_id)?;
            if commands.has_unresolved_cancel_for(&target) {
                return Ok(false);
            }
            let authorization = authorize_recovery_cancel(RecoveryCancelInput {
                binding: run.state.binding(),
                original_command_id: command_id,
                client_id,
                family,
                commands: &commands,
                proof: &proof,
                now_ms,
            })?;
            let guard = authority.dispatch_cancel(&authorization, now_ms)?;
            let _outcome =
                submit_recovery_cancel(&mut commands, private, authorization, now_ms, &guard);
            drop(guard);
            Ok(true)
        }
        CanaryRecoveryPlan::EmergencyFlatten { legs, .. } => {
            let Some(leg) = legs.first() else {
                return Ok(false);
            };
            if commands.has_unresolved_entry_or_reduce() {
                return Ok(false);
            }
            let public = PublicRest::production()?;
            let exchange_info = public.exchange_info()?;
            let instrument = parse_instrument(
                &exchange_info,
                run.state.binding().symbol.clone(),
                second.generation,
            )?;
            let depth = public.depth_snapshot(&run.state.binding().symbol, 5)?;
            let (bid, ask) = parse_depth_best_prices(&depth)?;
            let market_price =
                aggressive_recovery_price(leg.position_side, bid, ask, instrument.price_tick)?;
            let dispatch_at_ms = wall_clock_ms()?;
            let authorization = authorize_recovery_reduce(RecoveryReduceInput {
                binding: run.state.binding(),
                position_side: leg.position_side,
                quantity: leg.quantity,
                instrument: &instrument,
                market_price,
                market_price_valid_until_ms: dispatch_at_ms
                    .checked_add(500)
                    .ok_or(BinanceCanaryRecoveryError::Clock)?,
                commands: &commands,
                proof: &proof,
                now_ms: dispatch_at_ms,
            })?;
            let guard = authority.dispatch_reduce(&authorization, dispatch_at_ms)?;
            let _outcome = submit_recovery_reduce(
                &mut commands,
                private,
                authorization,
                dispatch_at_ms,
                &guard,
            );
            drop(guard);
            Ok(true)
        }
        CanaryRecoveryPlan::SealFlat { .. } | CanaryRecoveryPlan::RemainFenced { .. } => Ok(false),
    }
}

fn aggressive_recovery_price(
    side: PositionSide,
    bid: Price,
    ask: Price,
    tick: Price,
) -> Result<Price, BinanceCanaryRecoveryError> {
    let value = match side {
        PositionSide::Long => align_down(bid.value() * Decimal::new(99, 2), tick.value()),
        PositionSide::Short => align_up(ask.value() * Decimal::new(101, 2), tick.value()),
        PositionSide::Net => return Err(BinanceCanaryRecoveryError::MutationPlan),
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

fn push_unique(paths: &mut Vec<PathBuf>, path: &Path) {
    if !paths.iter().any(|existing| existing == path) {
        paths.push(path.to_path_buf());
    }
}

fn finalize_report(report: &mut BinanceCanaryRecoveryReport) {
    report
        .exact_cancel_required
        .retain(|path| !report.sealed_flat.contains(path));
    report
        .emergency_flatten_required
        .retain(|path| !report.sealed_flat.contains(path));
    report
        .remained_fenced
        .retain(|path| !report.sealed_flat.contains(path));
}

/// Revalidates every receipt-backed terminal before a new Canary may create a writer. Normal Flat
/// runs have no recovery marker and remain governed by their original evidence chain.
pub(super) fn validate_terminal_recovery_receipts(
    symbol_dir: &Path,
    private: &PrivateRest,
    now_ms: u64,
) -> Result<(), BinanceCanaryRecoveryError> {
    let mut runs = discover_runs(symbol_dir, now_ms)?;
    if runs.iter().any(|run| !run.state.is_terminal()) {
        return Err(BinanceCanaryRecoveryError::Unfinished);
    }
    let fenced =
        validate_and_bind_terminal_receipts(&mut runs, private, &private.recovery_signer_sha256())?;
    if fenced.is_empty() {
        Ok(())
    } else {
        Err(BinanceCanaryRecoveryError::UnresolvedCommandDebt)
    }
}

#[derive(Debug)]
struct DiscoveredRun {
    run_path: PathBuf,
    run_dir: PathBuf,
    state: CanaryRunState,
}

fn discover_runs(
    symbol_dir: &Path,
    now_ms: u64,
) -> Result<Vec<DiscoveredRun>, BinanceCanaryRecoveryError> {
    let mut runs = Vec::new();
    for entry in fs::read_dir(symbol_dir).map_err(|source| artifact(symbol_dir, source))? {
        let entry = entry.map_err(|source| artifact(symbol_dir, source))?;
        let file_type = entry
            .file_type()
            .map_err(|source| artifact(&entry.path(), source))?;
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        let run_dir = entry.path();
        let run_path = run_dir.join("run.json");
        let metadata = match fs::symlink_metadata(&run_path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(artifact(&run_path, source)),
        };
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(BinanceCanaryRecoveryError::Path);
        }
        let state = CanaryRunState::recover_existing(&run_path, now_ms)?;
        runs.push(DiscoveredRun {
            run_path,
            run_dir,
            state,
        });
    }
    runs.sort_by(|left, right| left.run_path.cmp(&right.run_path));
    Ok(runs)
}

#[derive(Clone, Debug)]
struct OwnedIdentity {
    owner_scope: String,
    command_id: String,
    family: NativeOrderFamily,
}

fn ownership_index(
    runs: &[DiscoveredRun],
) -> Result<BTreeMap<String, Option<OwnedIdentity>>, BinanceCanaryRecoveryError> {
    let mut result = BTreeMap::new();
    for run in runs {
        let journal = CommandJournal::open(run.run_dir.join("commands.jsonl"))?;
        for (command_id, owner, family, client_id) in journal.recovery_identities() {
            let identity = if owner.exchange == run.state.binding().exchange
                && owner.account == run.state.binding().account
                && owner.symbol == run.state.binding().symbol
            {
                Some(OwnedIdentity {
                    owner_scope: run.state.binding().owner_scope.clone(),
                    command_id: command_id.as_str().to_owned(),
                    family,
                })
            } else {
                None
            };
            result
                .entry(client_id.as_str().to_owned())
                .and_modify(|existing| *existing = None)
                .or_insert(identity);
        }
    }
    Ok(result)
}

#[derive(Clone, Debug)]
struct RawObservation {
    observed_at_ms: u64,
    positions: HedgePositionReadback,
    ordinary_orders: OrdinaryOrderReadback,
    algo_orders: AlgoOrderReadback,
}

fn observe(
    private: &PrivateRest,
    symbol: &Symbol,
    ownership: &BTreeMap<String, Option<OwnedIdentity>>,
) -> Result<RawObservation, BinanceCanaryRecoveryError> {
    let positions = private.position_readback(symbol)?;
    let orders = binance_private::parse_orders(&private.open_orders(symbol)?, symbol)?;
    let algo_ids =
        binance_private::parse_open_algo_client_ids(&private.open_algo_orders(symbol)?, symbol)?;
    Ok(RawObservation {
        observed_at_ms: wall_clock_ms()?,
        positions: normalize_positions(&positions),
        ordinary_orders: normalize_orders(&orders, ownership),
        algo_orders: normalize_algos(&algo_ids, ownership),
    })
}

fn normalize_positions(positions: &[crate::domain::Position]) -> HedgePositionReadback {
    let mut long = None;
    let mut short = None;
    for position in positions {
        match position.side {
            PositionSide::Long if long.replace(position.quantity).is_none() => {}
            PositionSide::Short if short.replace(position.quantity).is_none() => {}
            PositionSide::Long | PositionSide::Short | PositionSide::Net => {
                return HedgePositionReadback::Unknown;
            }
        }
    }
    match (long, short) {
        (Some(long_quantity), Some(short_quantity)) => HedgePositionReadback::Known {
            long_quantity,
            short_quantity,
        },
        _ => HedgePositionReadback::Unknown,
    }
}

fn has_unresolved_commands(run: &DiscoveredRun) -> Result<bool, BinanceCanaryRecoveryError> {
    Ok(CommandJournal::open(run.run_dir.join("commands.jsonl"))?.has_unresolved())
}

fn settle_terminal_safe_unknown_debt(
    runs: &mut [DiscoveredRun],
    fenced_paths: &[PathBuf],
    private: &PrivateRest,
) -> Result<(), BinanceCanaryRecoveryError> {
    for run in runs.iter_mut().filter(|run| {
        run.state.is_terminal() && fenced_paths.iter().any(|path| path == &run.run_path)
    }) {
        let mut commands = CommandJournal::open(run.run_dir.join("commands.jsonl"))?;
        let _ = commands.fence_interrupted_dispatches()?;
        let command_ids = commands.unknown_protection_or_cancel_command_ids();
        if command_ids.is_empty() {
            continue;
        }
        let mut facts = Journal::open(run.run_dir.join("facts.jsonl"))?;
        let mut reconciler = crate::execution::Reconciler::recover(&facts)?;
        for command_id in command_ids {
            let scoped = commands
                .receipt(&command_id)
                .and_then(|receipt| receipt.command.owner())
                .is_some_and(|owner| {
                    owner.exchange == run.state.binding().exchange
                        && owner.account == run.state.binding().account
                        && owner.symbol == run.state.binding().symbol
                });
            if !scoped {
                continue;
            }
            let _ = resolve_unknown_order_by_readback(
                &mut commands,
                private,
                &mut facts,
                &mut reconciler,
                &command_id,
                run.state.binding().readback_generation,
                wall_clock_ms()?,
            )?;
        }
    }
    Ok(())
}

fn normalize_orders(
    orders: &[crate::domain::Order],
    ownership: &BTreeMap<String, Option<OwnedIdentity>>,
) -> OrdinaryOrderReadback {
    let mut normalized = Vec::with_capacity(orders.len());
    for order in orders {
        let FieldState::Known(client_id) = &order.client_order_id else {
            return OrdinaryOrderReadback::Unknown;
        };
        let Some(Some(identity)) = ownership.get(client_id) else {
            normalized.push(foreign_ordinary(client_id));
            continue;
        };
        if identity.family != NativeOrderFamily::UmOrder {
            normalized.push(foreign_ordinary(client_id));
            continue;
        }
        normalized.push(RecoveryOrdinaryOrder {
            owner_scope: identity.owner_scope.clone(),
            command_id: identity.command_id.clone(),
            client_order_id: client_id.clone(),
        });
    }
    normalized.sort();
    OrdinaryOrderReadback::Known(normalized)
}

fn normalize_algos(
    client_ids: &[String],
    ownership: &BTreeMap<String, Option<OwnedIdentity>>,
) -> AlgoOrderReadback {
    let mut normalized = Vec::with_capacity(client_ids.len());
    for client_id in client_ids {
        let Some(Some(identity)) = ownership.get(client_id) else {
            normalized.push(foreign_algo(client_id));
            continue;
        };
        if identity.family != NativeOrderFamily::UmAlgo {
            normalized.push(foreign_algo(client_id));
            continue;
        }
        normalized.push(RecoveryAlgoOrder {
            owner_scope: identity.owner_scope.clone(),
            command_id: identity.command_id.clone(),
            client_algo_id: client_id.clone(),
        });
    }
    normalized.sort();
    AlgoOrderReadback::Known(normalized)
}

fn foreign_ordinary(client_id: &str) -> RecoveryOrdinaryOrder {
    RecoveryOrdinaryOrder {
        owner_scope: "foreign".to_owned(),
        command_id: foreign_id(client_id),
        client_order_id: client_id.to_owned(),
    }
}

fn foreign_algo(client_id: &str) -> RecoveryAlgoOrder {
    RecoveryAlgoOrder {
        owner_scope: "foreign".to_owned(),
        command_id: foreign_id(client_id),
        client_algo_id: client_id.to_owned(),
    }
}

fn foreign_id(client_id: &str) -> String {
    format!("foreign_{:x}", Sha256::digest(client_id.as_bytes()))
}

fn sign_readback(
    private: &PrivateRest,
    candidate: &CanaryRecoveryCandidate,
    raw: &RawObservation,
    ordinal: u8,
) -> Result<SignedCanaryReadback, BinanceCanaryRecoveryError> {
    let signer_sha256 = private.recovery_signer_sha256();
    let mut readback = SignedCanaryReadback {
        schema_version: CANARY_RECOVERY_SCHEMA_VERSION,
        readback_id: format!(
            "recovery_{}_{}_{}",
            raw.observed_at_ms, ordinal, candidate.binding.canary_id
        ),
        exchange: candidate.binding.exchange.clone(),
        account: candidate.binding.account.clone(),
        symbol: candidate.binding.symbol.clone(),
        generation: candidate.binding.readback_generation,
        observed_at_ms: raw.observed_at_ms,
        signer_sha256,
        payload_sha256: String::new(),
        signature_sha256: String::new(),
        signature_verified: false,
        positions: raw.positions.clone(),
        ordinary_orders: raw.ordinary_orders.clone(),
        algo_orders: raw.algo_orders.clone(),
    };
    readback.payload_sha256 = readback.calculate_payload_sha256()?;
    readback.signature_sha256 = private.sign_recovery_payload_sha256(&readback.payload_sha256)?;
    readback.signature_verified = private
        .verify_recovery_payload_signature(&readback.payload_sha256, &readback.signature_sha256);
    Ok(readback)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableFlatReceipt {
    schema_version: u16,
    binding: CanaryRunBinding,
    prior_revision: u64,
    prior_phase: CanaryRunPhase,
    first: SignedCanaryReadback,
    second: SignedCanaryReadback,
    sealed_at_ms: u64,
    receipt_sha256: String,
}

fn persist_receipt(
    run: &DiscoveredRun,
    first: SignedCanaryReadback,
    second: SignedCanaryReadback,
    plan: &CanaryRecoveryPlan,
) -> Result<DurableFlatReceipt, BinanceCanaryRecoveryError> {
    let CanaryRecoveryPlan::SealFlat { binding, .. } = plan else {
        return Err(BinanceCanaryRecoveryError::Receipt);
    };
    if binding != run.state.binding() {
        return Err(BinanceCanaryRecoveryError::Receipt);
    }
    if has_unresolved_commands(run)? {
        return Err(BinanceCanaryRecoveryError::UnresolvedCommandDebt);
    }
    let mut receipt = DurableFlatReceipt {
        schema_version: RECEIPT_SCHEMA_VERSION,
        binding: binding.clone(),
        prior_revision: run.state.revision(),
        prior_phase: run.state.phase().clone(),
        sealed_at_ms: second.observed_at_ms,
        first,
        second,
        receipt_sha256: String::new(),
    };
    receipt.receipt_sha256 = receipt_digest(&receipt)?;
    let path = run.run_dir.join(RECEIPT_FILE);
    if let Some(existing) = read_receipt(&path)? {
        return (existing == receipt)
            .then_some(existing)
            .ok_or(BinanceCanaryRecoveryError::ReceiptConflict);
    }
    let pending = run.run_dir.join(format!("{RECEIPT_FILE}.next"));
    if let Some(existing) = read_receipt(&pending)? {
        if existing != receipt {
            return Err(BinanceCanaryRecoveryError::ReceiptConflict);
        }
        promote_receipt(&pending, &path, &receipt)?;
        return Ok(receipt);
    }
    let bytes = serde_json::to_vec(&receipt)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)
        .map_err(|source| artifact(&pending, source))?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| artifact(&pending, source))?;
    promote_receipt(&pending, &path, &receipt)?;
    Ok(receipt)
}

fn resume_receipt(
    run: &mut DiscoveredRun,
    private: &PrivateRest,
    expected_signer: &str,
) -> Result<bool, BinanceCanaryRecoveryError> {
    if has_unresolved_commands(run)? {
        return Err(BinanceCanaryRecoveryError::UnresolvedCommandDebt);
    }
    let path = run.run_dir.join(RECEIPT_FILE);
    let Some(receipt) = read_receipt(&path)? else {
        let pending = run.run_dir.join(format!("{RECEIPT_FILE}.next"));
        let Some(receipt) = read_receipt(&pending)? else {
            return Ok(false);
        };
        validate_receipt(&receipt, run, private, expected_signer)?;
        promote_receipt(&pending, &path, &receipt)?;
        run.state
            .seal_recovered_flat(receipt.receipt_sha256, receipt.sealed_at_ms)?;
        return Ok(true);
    };
    validate_receipt(&receipt, run, private, expected_signer)?;
    run.state
        .seal_recovered_flat(receipt.receipt_sha256, receipt.sealed_at_ms)?;
    Ok(true)
}

fn validate_receipt(
    receipt: &DurableFlatReceipt,
    run: &DiscoveredRun,
    private: &PrivateRest,
    expected_signer: &str,
) -> Result<(), BinanceCanaryRecoveryError> {
    if receipt.schema_version != RECEIPT_SCHEMA_VERSION
        || receipt.binding != *run.state.binding()
        || receipt.prior_revision != run.state.revision()
        || receipt.prior_phase != *run.state.phase()
    {
        return Err(BinanceCanaryRecoveryError::Receipt);
    }
    validate_receipt_evidence(receipt, private, expected_signer)
}

fn validate_receipt_evidence(
    receipt: &DurableFlatReceipt,
    private: &PrivateRest,
    expected_signer: &str,
) -> Result<(), BinanceCanaryRecoveryError> {
    if receipt.schema_version != RECEIPT_SCHEMA_VERSION
        || receipt.receipt_sha256 != receipt_digest(receipt)?
        || receipt.sealed_at_ms != receipt.second.observed_at_ms
        || !private.verify_recovery_payload_signature(
            &receipt.first.payload_sha256,
            &receipt.first.signature_sha256,
        )
        || !private.verify_recovery_payload_signature(
            &receipt.second.payload_sha256,
            &receipt.second.signature_sha256,
        )
    {
        return Err(BinanceCanaryRecoveryError::Receipt);
    }
    let candidate = CanaryRecoveryCandidate {
        binding: receipt.binding.clone(),
        phase: receipt.prior_phase.clone(),
        frozen: false,
    };
    if !matches!(
        plan_canary_recovery(&candidate, expected_signer, &receipt.first, &receipt.second),
        CanaryRecoveryPlan::SealFlat { .. }
    ) {
        return Err(BinanceCanaryRecoveryError::Receipt);
    }
    Ok(())
}

fn validate_and_bind_terminal_receipts(
    runs: &mut [DiscoveredRun],
    private: &PrivateRest,
    expected_signer: &str,
) -> Result<Vec<PathBuf>, BinanceCanaryRecoveryError> {
    let mut fenced = Vec::new();
    for run in runs.iter_mut().filter(|run| run.state.is_terminal()) {
        let primary = run.run_dir.join(RECEIPT_FILE);
        let pending = run.run_dir.join(format!("{RECEIPT_FILE}.next"));
        let receipt = match (read_receipt(&primary)?, read_receipt(&pending)?) {
            (Some(primary), Some(pending)) if primary != pending => {
                return Err(BinanceCanaryRecoveryError::ReceiptConflict);
            }
            (Some(primary), _) => Some(primary),
            (None, Some(pending)) => {
                promote_receipt(
                    &run.run_dir.join(format!("{RECEIPT_FILE}.next")),
                    &primary,
                    &pending,
                )?;
                Some(pending)
            }
            (None, None) => None,
        };
        let marker = run.state.recovery_receipt_sha256().map(str::to_owned);
        let Some(receipt) = receipt else {
            if marker.is_some() {
                return Err(BinanceCanaryRecoveryError::Receipt);
            }
            continue;
        };
        validate_receipt_evidence(&receipt, private, expected_signer)?;
        let flat_matches = matches!(
            run.state.phase(),
            CanaryRunPhase::Flat { readback_sha256 }
                if readback_sha256 == &receipt.receipt_sha256
        );
        if receipt.binding != *run.state.binding()
            || run.state.revision() <= receipt.prior_revision
            || !flat_matches
            || marker
                .as_deref()
                .is_some_and(|value| value != receipt.receipt_sha256)
        {
            return Err(BinanceCanaryRecoveryError::Receipt);
        }
        if has_unresolved_commands(run)? {
            push_unique(&mut fenced, &run.run_path);
            continue;
        }
        if marker.is_none() {
            run.state
                .bind_existing_recovery_receipt(receipt.receipt_sha256)?;
        }
    }
    Ok(fenced)
}

fn read_receipt(path: &Path) -> Result<Option<DurableFlatReceipt>, BinanceCanaryRecoveryError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(artifact(path, source)),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(BinanceCanaryRecoveryError::Path);
    }
    let bytes = fs::read(path).map_err(|source| artifact(path, source))?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn promote_receipt(
    pending: &Path,
    primary: &Path,
    expected: &DurableFlatReceipt,
) -> Result<(), BinanceCanaryRecoveryError> {
    match fs::hard_link(pending, primary) {
        Ok(()) => {
            let _ = fs::remove_file(pending);
            Ok(())
        }
        Err(_) => match read_receipt(primary)? {
            Some(existing) if existing == *expected => {
                let _ = fs::remove_file(pending);
                Ok(())
            }
            Some(_) => Err(BinanceCanaryRecoveryError::ReceiptConflict),
            None => Err(BinanceCanaryRecoveryError::Receipt),
        },
    }
}

fn receipt_digest(receipt: &DurableFlatReceipt) -> Result<String, serde_json::Error> {
    serde_json::to_vec(&(
        "venue.canary.recovery.flat-receipt.v1",
        receipt.schema_version,
        &receipt.binding,
        receipt.prior_revision,
        &receipt.prior_phase,
        &receipt.first,
        &receipt.second,
        receipt.sealed_at_ms,
    ))
    .map(|bytes| format!("{:x}", Sha256::digest(bytes)))
}

struct RecoveryLock {
    file: File,
}

impl RecoveryLock {
    fn acquire(path: &Path) -> Result<Self, BinanceCanaryRecoveryError> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| artifact(path, source))?;
        file.try_lock_exclusive()
            .map_err(BinanceCanaryRecoveryError::Lock)?;
        Ok(Self { file })
    }
}

impl Drop for RecoveryLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

fn wall_clock_ms() -> Result<u64, BinanceCanaryRecoveryError> {
    let value = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceCanaryRecoveryError::Clock)?
        .as_millis();
    u64::try_from(value).map_err(|_| BinanceCanaryRecoveryError::Clock)
}

fn artifact(path: &Path, source: std::io::Error) -> BinanceCanaryRecoveryError {
    BinanceCanaryRecoveryError::Artifact {
        path: path.to_path_buf(),
        source,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BinanceCanaryRecoveryError {
    #[error("Canary recovery artifact root or child path is invalid")]
    Path,
    #[error("another Canary recovery process owns the symbol lock: {0}")]
    Lock(#[source] std::io::Error),
    #[error("system clock is unavailable for Canary recovery")]
    Clock,
    #[error("Canary recovery receipt is invalid")]
    Receipt,
    #[error("Canary recovery receipt conflicts with an existing receipt")]
    ReceiptConflict,
    #[error("unfinished Canary run still requires recovery")]
    Unfinished,
    #[error("Canary recovery mutation loop exceeded its bounded attempt count")]
    MutationLimit,
    #[error("Canary recovery mutation plan is invalid")]
    MutationPlan,
    #[error("Canary recovery cannot seal while its command journal has unresolved debt")]
    UnresolvedCommandDebt,
    #[error("Canary recovery artifact access failed for {path}: {source}", path = path.display())]
    Artifact {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    Private(#[from] crate::exchange::binance::PrivateError),
    #[error(transparent)]
    PrivateReadback(#[from] crate::exchange::binance::PrivateReadbackError),
    #[error(transparent)]
    PrivateParse(#[from] crate::exchange::binance_private::PrivateParseError),
    #[error(transparent)]
    Public(#[from] crate::exchange::binance::PublicError),
    #[error(transparent)]
    Binance(#[from] crate::exchange::binance::BinanceError),
    #[error(transparent)]
    RecoveryWriter(#[from] crate::execution::RecoveryWriterError),
    #[error(transparent)]
    Writer(#[from] crate::execution::WriterLeaseError),
    #[error("Canary recovery account writer registry failed closed: {reason}")]
    WriterRegistry { reason: String },
    #[error(transparent)]
    Execution(#[from] crate::execution::ExecutionError),
    #[error(transparent)]
    Command(#[from] crate::domain::CommandError),
    #[error(transparent)]
    Amount(#[from] crate::domain::AmountError),
    #[error(transparent)]
    Run(#[from] crate::execution::CanaryRunStateError),
    #[error(transparent)]
    Journal(#[from] crate::execution::CommandJournalError),
    #[error(transparent)]
    Reconciliation(#[from] crate::execution::ReconciliationError),
    #[error(transparent)]
    Storage(#[from] crate::storage::StorageError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::domain::{
        OrderCommand, OrderOwner, OrderPurpose, OrderSide, Position, PositionSide, Price,
    };

    #[test]
    fn durable_receipt_resumes_after_crash_before_run_state_seal()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let run_dir = directory.path().join("protection_1");
        fs::create_dir(&run_dir)?;
        let run_path = run_dir.join("run.json");
        let binding = binding()?;
        let state = CanaryRunState::create_new(&run_path, binding.clone(), 100)?;
        let private = PrivateRest::recovery_test_client("api-key", b"secret")?;
        let expected_signer = private.recovery_signer_sha256();
        let candidate = CanaryRecoveryCandidate {
            binding,
            phase: state.phase().clone(),
            frozen: false,
        };
        let first = sign_readback(&private, &candidate, &flat_raw(101), 1)?;
        let second = sign_readback(&private, &candidate, &flat_raw(102), 2)?;
        let plan = plan_canary_recovery(&candidate, &expected_signer, &first, &second);
        assert!(matches!(plan, CanaryRecoveryPlan::SealFlat { .. }));
        let run = DiscoveredRun {
            run_path: run_path.clone(),
            run_dir: run_dir.clone(),
            state,
        };
        let receipt = persist_receipt(&run, first, second, &plan)?;
        assert!(run_dir.join(RECEIPT_FILE).exists());
        assert_eq!(receipt.prior_revision, 1);

        let mut recovered = DiscoveredRun {
            run_path: run_path.clone(),
            run_dir,
            state: CanaryRunState::recover_existing(&run_path, 200)?,
        };
        assert!(resume_receipt(&mut recovered, &private, &expected_signer)?);
        assert!(recovered.state.is_terminal());
        assert_eq!(recovered.state.revision(), 2);
        fs::remove_file(recovered.run_dir.join(RECEIPT_FILE))?;
        assert!(validate_terminal_recovery_receipts(directory.path(), &private, 300).is_err());
        Ok(())
    }

    #[test]
    fn terminal_flat_writer_retirement_requires_a_new_clean_signed_pair()
    -> Result<(), Box<dyn std::error::Error>> {
        let private = PrivateRest::recovery_test_client("api-key", b"secret")?;
        let candidate = CanaryRecoveryCandidate {
            binding: binding()?,
            phase: CanaryRunPhase::Flat {
                readback_sha256: "a".repeat(64),
            },
            frozen: true,
        };
        let first = sign_readback(&private, &candidate, &flat_raw(101), 1)?;
        let second = sign_readback(&private, &candidate, &flat_raw(102), 2)?;
        assert!(matches!(
            plan_terminal_flat_writer_retirement(
                &candidate,
                &private.recovery_signer_sha256(),
                &first,
                &second,
            ),
            CanaryRecoveryPlan::SealFlat { .. }
        ));

        let unknown = RawObservation {
            algo_orders: AlgoOrderReadback::Unknown,
            ..flat_raw(103)
        };
        let unknown_second = sign_readback(&private, &candidate, &unknown, 2)?;
        assert!(matches!(
            plan_terminal_flat_writer_retirement(
                &candidate,
                &private.recovery_signer_sha256(),
                &first,
                &unknown_second,
            ),
            CanaryRecoveryPlan::RemainFenced { .. }
        ));
        Ok(())
    }

    #[test]
    fn tampered_recovery_receipt_never_seals_run() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let run_dir = directory.path().join("protection_1");
        fs::create_dir(&run_dir)?;
        let run_path = run_dir.join("run.json");
        let binding = binding()?;
        let state = CanaryRunState::create_new(&run_path, binding.clone(), 100)?;
        let private = PrivateRest::recovery_test_client("api-key", b"secret")?;
        let expected_signer = private.recovery_signer_sha256();
        let candidate = CanaryRecoveryCandidate {
            binding,
            phase: state.phase().clone(),
            frozen: false,
        };
        let first = sign_readback(&private, &candidate, &flat_raw(101), 1)?;
        let second = sign_readback(&private, &candidate, &flat_raw(102), 2)?;
        let plan = plan_canary_recovery(&candidate, &expected_signer, &first, &second);
        let run = DiscoveredRun {
            run_path: run_path.clone(),
            run_dir: run_dir.clone(),
            state,
        };
        let _ = persist_receipt(&run, first, second, &plan)?;
        let receipt_path = run_dir.join(RECEIPT_FILE);
        let mut payload = fs::read_to_string(&receipt_path)?;
        payload = payload.replacen(&expected_signer, &"0".repeat(64), 1);
        fs::write(&receipt_path, payload)?;

        let mut recovered = DiscoveredRun {
            run_path: run_path.clone(),
            run_dir,
            state: CanaryRunState::recover_existing(&run_path, 200)?,
        };
        assert!(resume_receipt(&mut recovered, &private, &expected_signer).is_err());
        assert!(!recovered.state.is_terminal());
        Ok(())
    }

    #[test]
    fn unknown_exchange_identity_is_preserved_as_foreign_debt()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDT".parse()?;
        let orders = binance_private::parse_orders(
            r#"[{"symbol":"SOLUSDT","orderId":"1","clientOrderId":"outside","status":"NEW","side":"BUY","positionSide":"LONG","origQty":"0.1","executedQty":"0","price":"100","avgPrice":"0","reduceOnly":false}]"#,
            &symbol,
        )?;
        let normalized = normalize_orders(&orders, &BTreeMap::new());
        let OrdinaryOrderReadback::Known(orders) = normalized else {
            return Err(std::io::Error::other("ordinary orders unexpectedly unknown").into());
        };
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].owner_scope, "foreign");
        Ok(())
    }

    #[test]
    fn hedge_position_normalization_requires_exactly_one_long_and_short_leg()
    -> Result<(), Box<dyn std::error::Error>> {
        let symbol: Symbol = "SOL/USDT".parse()?;
        let leg = |side| Position {
            symbol: symbol.clone(),
            side,
            quantity: Decimal::ZERO,
            entry_price: None,
            mark_price: None,
        };
        assert_eq!(normalize_positions(&[]), HedgePositionReadback::Unknown);
        assert_eq!(
            normalize_positions(&[leg(PositionSide::Long)]),
            HedgePositionReadback::Unknown
        );
        assert_eq!(
            normalize_positions(&[
                leg(PositionSide::Long),
                leg(PositionSide::Long),
                leg(PositionSide::Short),
            ]),
            HedgePositionReadback::Unknown
        );
        assert_eq!(
            normalize_positions(&[leg(PositionSide::Long), leg(PositionSide::Short)]),
            HedgePositionReadback::Known {
                long_quantity: Decimal::ZERO,
                short_quantity: Decimal::ZERO,
            }
        );
        Ok(())
    }

    #[test]
    fn unresolved_wal_prevents_a_flat_recovery_receipt() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempdir()?;
        let run_dir = directory.path().join("protection_1");
        fs::create_dir(&run_dir)?;
        let run_path = run_dir.join("run.json");
        let binding = binding()?;
        let state = CanaryRunState::create_new(&run_path, binding.clone(), 100)?;
        let mut commands = CommandJournal::open(run_dir.join("commands.jsonl"))?;
        commands.prepare_place(OrderCommand {
            command_id: CommandId::new("entry_unresolved")?,
            client_order_id: CommandId::new("entry_unresolved_client")?,
            owner: OrderOwner {
                strategy_instance_id: "manual_canary".to_owned(),
                run_id: "protection_1".to_owned(),
                exchange: "binance".to_owned(),
                account: "portfolio_margin_um".to_owned(),
                symbol: binding.symbol.clone(),
                purpose: OrderPurpose::Entry,
            },
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity: Decimal::new(1, 2),
            limit_price: Price::new(Decimal::new(100, 0))?,
            reduce_only: false,
        })?;
        let private = PrivateRest::recovery_test_client("api-key", b"secret")?;
        let candidate = CanaryRecoveryCandidate {
            binding,
            phase: state.phase().clone(),
            frozen: false,
        };
        let first = sign_readback(&private, &candidate, &flat_raw(101), 1)?;
        let second = sign_readback(&private, &candidate, &flat_raw(102), 2)?;
        let plan = plan_canary_recovery(
            &candidate,
            &private.recovery_signer_sha256(),
            &first,
            &second,
        );
        let run = DiscoveredRun {
            run_path,
            run_dir,
            state,
        };
        assert!(matches!(
            persist_receipt(&run, first, second, &plan),
            Err(BinanceCanaryRecoveryError::UnresolvedCommandDebt)
        ));
        Ok(())
    }

    fn binding() -> Result<CanaryRunBinding, Box<dyn std::error::Error>> {
        Ok(CanaryRunBinding {
            canary_id: "protection_1".to_owned(),
            exchange: "binance".to_owned(),
            account: "portfolio_margin_um".to_owned(),
            symbol: "SOL/USDT".parse()?,
            owner_scope: "manual_canary_mainnet".to_owned(),
            release_id: "stage4_manual_canary_v1".to_owned(),
            position_side: PositionSide::Long,
            writer_generation: 1,
            readback_generation: 1,
            valid_until_ms: 10_000,
        })
    }

    fn flat_raw(observed_at_ms: u64) -> RawObservation {
        RawObservation {
            observed_at_ms,
            positions: HedgePositionReadback::Known {
                long_quantity: Decimal::ZERO,
                short_quantity: Decimal::ZERO,
            },
            ordinary_orders: OrdinaryOrderReadback::Known(Vec::new()),
            algo_orders: AlgoOrderReadback::Known(Vec::new()),
        }
    }
}
