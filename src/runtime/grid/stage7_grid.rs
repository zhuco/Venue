use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::{
    backoff::jittered_exponential_delay_ms,
    config::{BitgetAccountBinding, Config, GateAccountBinding},
    domain::{
        CancelCommand, CommandId, ExecutionCommand, FieldState, MarketOrderCommand, Order,
        OrderOwner, OrderPurpose, OrderSide, OrderState, Position, PositionSide, Price,
    },
    exchange::grid::{
        BinanceGridVenue, BitgetGridVenue, GateGridVenue, GridPrivateEvent, GridVenueError,
        GridVenueFill, GridVenueReadback, HedgedGridVenue, physical_notional,
    },
    execution::{
        Capability, CapabilityBinding, CapabilityEvidenceError, CapabilityEvidenceStore,
        CapabilityProbe, CommandJournal, CommandJournalError, CommandState,
        ExternalAlgoCancelInput, ExternalAlgoCleanupJournal, ExternalAlgoCleanupState,
        ExternalAlgoCustody, FlatReceipt, RecoveryObservationProof, RecoveryWriterAuthority,
        RecoveryWriterScope, WriterLeaseAuthority, WriterLeaseError, WriterScope, WriterSession,
        authorize_external_algo_cancel, sha256_hex, submit_external_algo_cancel,
    },
    storage::{
        PrivateEvidence, PrivateEvidenceError, PrivateEvidenceJournal, ProjectionStore,
        StorageError,
    },
    strategy::hedged_grid::{
        GridAction, GridDecision, GridEpoch, GridInventory, GridOrderIntent, GridOrderKey,
        GridPhase, GridPosition, GridResetReason, HedgedGridBinding, HedgedGridError,
        HedgedGridParams, HedgedGridState,
    },
};

use super::{
    HedgedGridControlTarget, HedgedGridLiveError,
    hedged_grid_live::{
        GridMutation, align_up, client_order_id, highest_durable_replenishment_round,
        market_command, next_cancel_command, parse_grid_client_order_id, place_command,
    },
    stage7_writer_registry,
};

const CONTROL_FILE: &str = "hedged_grid_control.json";
const CHECKPOINT_FILE: &str = "hedged_grid_state.json";
const ORDER_HEALTH_FILE: &str = "hedged_grid_order_health.json";
const COMMAND_FILE: &str = "commands.jsonl";
const WRITER_FILE: &str = "writer.json";
const PRIVATE_EVIDENCE_FILE: &str = "private_evidence.jsonl";
const CAPABILITY_EVIDENCE_FILE: &str = "capabilities.jsonl";
const PRIVATE_READBACK_INTERVAL_MS: u64 = 10 * 60 * 1_000;
const ORDER_HEALTH_INTERVAL_MS: u64 = 30 * 60 * 1_000;
const IDLE_SLEEP_MS: u64 = 100;
const MAX_PRIVATE_EVENTS_PER_TURN: usize = 128;
const BITGET_FILL_HISTORY_OVERLAP_MS: u64 = 5 * 60 * 1_000;
const REJECTED_CANCEL_RETRY_MS: u64 = 250;
const REJECTED_GRID_RESET_DELAY_MS: u64 = 30_000;
const VENUE_STARTUP_RETRY_BASE_MS: u64 = 250;
const VENUE_STARTUP_RETRY_CAP_MS: u64 = 5_000;
const CANARY_CAPABILITY_VALIDITY_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
const SINGLE_ORDER_MAX_NOTIONAL: Decimal = Decimal::from_parts(6, 0, 0, false, 0);
const BINANCE_LEGACY_GRID_ORDER_MAX_NOTIONAL: Decimal = Decimal::from_parts(625, 0, 0, false, 2);
const REPLENISHMENT_MAX_NOTIONAL: Decimal = Decimal::from_parts(18, 0, 0, false, 0);

#[path = "binance_legacy_stage7_bridge.rs"]
mod binance_legacy_stage7_bridge;
#[path = "stage7_epoch_install.rs"]
mod stage7_epoch_install;
#[path = "stage7_exposure.rs"]
mod stage7_exposure;
#[path = "stage7_exposure_shadow_verifier.rs"]
mod stage7_exposure_shadow_verifier;
#[path = "stage7_flatten.rs"]
mod stage7_flatten;
#[path = "stage7_grid_error.rs"]
mod stage7_grid_error;
#[path = "stage7_grid_model.rs"]
mod stage7_grid_model;
#[path = "stage7_inventory_recovery_evidence.rs"]
mod stage7_inventory_recovery_evidence;
#[path = "stage7_mutation.rs"]
mod stage7_mutation;
#[path = "stage7_private_evidence_recovery.rs"]
mod stage7_private_evidence_recovery;
#[path = "stage7_public_evidence_recovery.rs"]
mod stage7_public_evidence_recovery;
pub use binance_legacy_stage7_bridge::{
    BinanceLegacyStage7BridgeReport, BinanceLegacyStage7BridgeRequest,
    BinanceLegacyStage7StopRequest, request_binance_legacy_stage7_stop,
    run_binance_legacy_stage7_bridge,
};
#[cfg(test)]
use stage7_epoch_install::install_epoch;
use stage7_epoch_install::install_epoch_with_public_refresh;
pub use stage7_exposure_shadow_verifier::{
    ExposureShadowLaneReport, ExposureShadowVerificationError, ExposureShadowVerificationReport,
    ExposureShadowVerifiedDecision, ExposureShadowVerifiedReason, VerifiedRawRiskEvidenceRef,
    verify_stage7_exposure_shadow_evidence,
};
pub use stage7_flatten::{
    Stage7FlattenReport, Stage7FlattenRequest, run_binance_stage7_flatten,
    run_bitget_stage7_flatten, run_gate_stage7_flatten,
};
pub use stage7_grid_error::Stage7GridError;
pub use stage7_grid_model::{
    Stage7CanaryRecoveryReport, Stage7CanaryReport, Stage7CanaryRequest,
    Stage7ExecutableHandoffReport, Stage7ExecutableHandoffRequest, Stage7GridCanaryReport,
    Stage7GridReport, Stage7GridRequest,
};
use stage7_grid_model::{Stage7GridCheckpoint, Stage7GridControl};
pub use stage7_inventory_recovery_evidence::{
    InventoryRecoveryAcceptanceReport, InventoryRecoveryEvidenceError,
    verify_stage7_inventory_recovery_evidence,
};
use stage7_mutation::Stage7Mutation;
pub use stage7_private_evidence_recovery::{
    Stage7PrivateEvidenceRecoveryReport, Stage7PrivateEvidenceRecoveryRequest,
    recover_stage7_private_evidence,
};
use stage7_private_evidence_recovery::{
    open_stage7_private_evidence, stage7_private_evidence_path, stage7_private_evidence_snapshot,
    verify_stage7_private_evidence_snapshot,
};
pub(super) use stage7_public_evidence_recovery::stage7_public_evidence_path;
pub use stage7_public_evidence_recovery::{
    Stage7PublicEvidenceRecoveryReport, Stage7PublicEvidenceRecoveryRequest,
    recover_stage7_public_evidence,
};
#[path = "stage7_canary_contract.rs"]
mod stage7_canary_contract;
#[path = "stage7_canary_limit.rs"]
mod stage7_canary_limit;
#[path = "stage7_canary_safety.rs"]
mod stage7_canary_safety;
#[path = "stage7_canary_support.rs"]
mod stage7_canary_support;
#[path = "stage7_executable_handoff.rs"]
mod stage7_executable_handoff;
pub use stage7_executable_handoff::{
    run_binance_stage7_executable_handoff, run_bitget_stage7_executable_handoff,
    run_gate_stage7_executable_handoff,
};
#[path = "stage7_external_algo_cleanup.rs"]
mod stage7_external_algo_cleanup;
pub use stage7_external_algo_cleanup::{
    Stage7ExternalAlgoCleanupReport, Stage7ExternalAlgoCleanupRequest,
    run_binance_stage7_external_algo_cleanup,
};
#[path = "stage7_grid_canary.rs"]
mod stage7_grid_canary;
#[path = "stage7_health.rs"]
mod stage7_health;
#[path = "stage7_public_runtime.rs"]
mod stage7_public_runtime;
#[path = "stage7_retry.rs"]
mod stage7_retry;
#[path = "stage7_risk_lane.rs"]
mod stage7_risk_lane;
use stage7_canary_contract::Stage7CanaryVenue;
use stage7_canary_support::{
    append_stage7_canary_capabilities, canary_cleanup_readback, canary_owner, canary_preflight,
    canary_quantity, canary_readback, command_id, reduce_canary_market, require_stage7_canary,
    require_stage7_grid_lifecycle, stage7_balance_asset_matches_binding,
    wait_for_canary_cleanup_position, wait_for_canary_position,
};
pub use stage7_grid_canary::{
    run_binance_stage7_grid_canary, run_bitget_stage7_grid_canary, run_gate_stage7_grid_canary,
};
use stage7_public_runtime::Stage7PublicRuntime;
use stage7_retry::{is_transient_instrument_rule_error, is_transient_readback_error};
use stage7_risk_lane::Stage7RiskLane;

pub fn set_stage7_grid_control(
    cfg: &Config,
    artifacts_root: &Path,
    target: HedgedGridControlTarget,
) -> Result<(), Stage7GridError> {
    if !artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    fs::create_dir_all(artifacts_root).map_err(|source| Stage7GridError::Io {
        path: artifacts_root.to_path_buf(),
        source,
    })?;
    let binding = stage7_binding(cfg)?;
    let control_store = ProjectionStore::new(artifacts_root.join(CONTROL_FILE));
    if let Some(existing) = control_store.load::<Stage7GridControl>()?
        && (existing.schema_version != 1 || existing.binding != binding)
    {
        return Err(Stage7GridError::Control);
    }
    if let Some(existing) =
        ProjectionStore::new(artifacts_root.join(CHECKPOINT_FILE)).load::<Stage7GridCheckpoint>()?
        && (existing.schema_version != 1 || existing.binding != binding)
    {
        return Err(Stage7GridError::Checkpoint);
    }
    control_store.save(&Stage7GridControl {
        schema_version: 1,
        binding,
        target,
    })?;
    Ok(())
}

pub fn run_gate_stage7_grid(
    cfg: &Config,
    request: Stage7GridRequest,
) -> Result<Stage7GridReport, Stage7GridError> {
    if !request.shadow_only && !request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let binding = gate_binding(cfg)?;
    let mut venue = if request.shadow_only {
        GateGridVenue::production(binding.symbol.clone(), 1)?
    } else {
        open_live_stage7_venue(&request, &binding, "gate", || {
            GateGridVenue::production(binding.symbol.clone(), 1)
        })?
    };
    if !request.shadow_only {
        let params = release_params(cfg, &binding)?;
        require_stage7_canary(
            &venue,
            &binding,
            &params,
            &request.artifacts_root,
            wall_clock_ms()?,
        )?;
        require_stage7_grid_lifecycle(
            &venue,
            &binding,
            &params,
            cfg.hedged_grid.and_then(|grid| grid.exposure_take_profit),
            &request.artifacts_root,
            wall_clock_ms()?,
        )?;
    }
    run_stage7_grid(cfg, request, binding, &mut venue)
}

/// Runs Binance through the shared resident without mutation while its existing server root is
/// still owned by the legacy deployment. Live admission and checkpoint handoff remain an explicit
/// later gate; this path proves adapter/resident compatibility without creating a second writer.
pub fn run_binance_shared_grid_shadow(
    cfg: &Config,
    request: Stage7GridRequest,
) -> Result<Stage7GridReport, Stage7GridError> {
    if !request.shadow_only || request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let binding = binance_binding(cfg)?;
    let mut venue = BinanceGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_grid(cfg, request, binding, &mut venue)
}

pub fn run_binance_stage7_grid(
    cfg: &Config,
    request: Stage7GridRequest,
) -> Result<Stage7GridReport, Stage7GridError> {
    if !request.shadow_only && !request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let binding = binance_binding(cfg)?;
    let mut venue = if request.shadow_only {
        BinanceGridVenue::production(binding.symbol.clone(), 1)?
    } else {
        open_live_stage7_venue(&request, &binding, "binance", || {
            BinanceGridVenue::production(binding.symbol.clone(), 1)
        })?
    };
    if !request.shadow_only {
        let params = release_params(cfg, &binding)?;
        let now_ms = wall_clock_ms()?;
        let executable = stage7_canary_support::executable_sha256()?;
        let exposure_take_profit_sha256 = stage7_canary_support::exposure_release_digest(
            cfg.hedged_grid.and_then(|grid| grid.exposure_take_profit),
        )?;
        if !binance_legacy_stage7_bridge::require_binance_legacy_bridge_admission(
            &venue,
            &binding,
            &params,
            &exposure_take_profit_sha256,
            &request.artifacts_root,
            &executable,
            now_ms,
        )? {
            require_stage7_canary(&venue, &binding, &params, &request.artifacts_root, now_ms)?;
            require_stage7_grid_lifecycle(
                &venue,
                &binding,
                &params,
                cfg.hedged_grid.and_then(|grid| grid.exposure_take_profit),
                &request.artifacts_root,
                now_ms,
            )?;
        }
    }
    run_stage7_grid(cfg, request, binding, &mut venue)
}

pub fn run_bitget_stage7_grid(
    cfg: &Config,
    request: Stage7GridRequest,
) -> Result<Stage7GridReport, Stage7GridError> {
    if !request.shadow_only && !request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let binding = bitget_binding(cfg)?;
    let mut venue = if request.shadow_only {
        BitgetGridVenue::production(binding.symbol.clone(), 1)?
    } else {
        open_live_stage7_venue(&request, &binding, "bitget", || {
            BitgetGridVenue::production(binding.symbol.clone(), 1)
        })?
    };
    if !request.shadow_only {
        let params = release_params(cfg, &binding)?;
        require_stage7_canary(
            &venue,
            &binding,
            &params,
            &request.artifacts_root,
            wall_clock_ms()?,
        )?;
        require_stage7_grid_lifecycle(
            &venue,
            &binding,
            &params,
            cfg.hedged_grid.and_then(|grid| grid.exposure_take_profit),
            &request.artifacts_root,
            wall_clock_ms()?,
        )?;
    }
    run_stage7_grid(cfg, request, binding, &mut venue)
}

fn open_live_stage7_venue<V, F>(
    request: &Stage7GridRequest,
    binding: &HedgedGridBinding,
    exchange: &'static str,
    mut open: F,
) -> Result<V, Stage7GridError>
where
    V: HedgedGridVenue,
    F: FnMut() -> Result<V, GridVenueError>,
{
    let mut failures = 0_u8;
    loop {
        let attempt = open().and_then(|mut venue| {
            venue.verify_current_instrument_rules()?;
            Ok(venue)
        });
        match attempt {
            Ok(venue) => return Ok(venue),
            Err(error) if stage7_retry::is_transient_venue_startup_error(&error) => {
                failures = failures.saturating_add(1);
                let control_store = ProjectionStore::new(request.artifacts_root.join(CONTROL_FILE));
                if read_control(&control_store, binding)? == HedgedGridControlTarget::Stop {
                    return Err(Stage7GridError::StartupStopped);
                }
                warn!(
                    event = "stage7_venue_startup_backoff",
                    exchange,
                    reason = %error,
                    "venue startup is temporarily unavailable; retaining the closed mutation gate"
                );
                let delay = jittered_exponential_delay_ms(
                    VENUE_STARTUP_RETRY_BASE_MS,
                    VENUE_STARTUP_RETRY_CAP_MS,
                    failures,
                    &binding.account,
                    wall_clock_ms()?,
                );
                thread::sleep(Duration::from_millis(delay));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[path = "stage7_canary_runtime.rs"]
mod stage7_canary_runtime;
use stage7_canary_runtime::run_stage7_canary_recovery;
pub use stage7_canary_runtime::{
    run_binance_stage7_canary, run_binance_stage7_canary_recovery, run_bitget_stage7_canary,
    run_bitget_stage7_canary_recovery, run_gate_stage7_canary, run_gate_stage7_canary_recovery,
};

#[path = "stage7_resident.rs"]
mod stage7_resident;
use stage7_resident::run_stage7_grid;

#[path = "stage7_grid_binding.rs"]
mod stage7_grid_binding;
use stage7_grid_binding::*;

fn release_params(
    cfg: &Config,
    binding: &HedgedGridBinding,
) -> Result<HedgedGridParams, Stage7GridError> {
    let grid = cfg.hedged_grid.ok_or(Stage7GridError::GridConfig)?;
    HedgedGridParams::fixed_release(
        binding
            .symbol
            .quote()
            .parse()
            .map_err(|_| Stage7GridError::Binding)?,
        grid.grid_count,
    )
    .map_err(Stage7GridError::Strategy)
}

fn read_control(
    store: &ProjectionStore,
    binding: &HedgedGridBinding,
) -> Result<HedgedGridControlTarget, Stage7GridError> {
    match store.load::<Stage7GridControl>()? {
        None => Ok(HedgedGridControlTarget::Stop),
        Some(control) if control.schema_version == 1 && control.binding == *binding => {
            Ok(control.target)
        }
        Some(_) => Err(Stage7GridError::Control),
    }
}

fn initialize_control_for_new_root(
    control_store: &ProjectionStore,
    checkpoint_store: &ProjectionStore,
    binding: &HedgedGridBinding,
) -> Result<(), Stage7GridError> {
    if control_store.load::<Stage7GridControl>()?.is_some()
        || checkpoint_store.load::<Stage7GridCheckpoint>()?.is_some()
    {
        return Ok(());
    }
    control_store.save(&Stage7GridControl {
        schema_version: 1,
        binding: binding.clone(),
        target: HedgedGridControlTarget::Running,
    })?;
    Ok(())
}

fn load_checkpoint(
    store: &ProjectionStore,
    binding: &HedgedGridBinding,
    params: &HedgedGridParams,
    reset_on_start: bool,
) -> Result<Stage7GridCheckpoint, Stage7GridError> {
    let mut checkpoint = match store.load::<Stage7GridCheckpoint>()? {
        None => Stage7GridCheckpoint {
            schema_version: 1,
            binding: binding.clone(),
            state: HedgedGridState::new_with_params(binding.clone(), params.clone())?,
            private_generation: 0,
            exposure_guard: None,
            pending_exposure_reduction: None,
            fill_history_start_ms: wall_clock_ms()?,
            order_health_fenced: false,
            last_order_health_checked_at_ms: 0,
        },
        Some(checkpoint)
            if checkpoint.schema_version == 1
                && checkpoint.binding == *binding
                && checkpoint.state.binding == *binding =>
        {
            checkpoint
        }
        Some(_) => return Err(Stage7GridError::Checkpoint),
    };
    if checkpoint.fill_history_start_ms == 0 {
        // Existing checkpoints predate the account-wide Bitget fill fence. No Stage-7 Bitget
        // Live release existed before it, so anchor this root at its first safe restart instead
        // of recording unrelated account history as a grid fact.
        checkpoint.fill_history_start_ms = wall_clock_ms()?;
    }
    checkpoint.state.migrate_checkpoint()?;
    checkpoint.state.reconcile_order_sequences();
    if checkpoint.state.params != *params {
        if !reset_on_start {
            return Err(Stage7GridError::ParameterChange);
        }
        checkpoint.state.params = params.clone();
        let _ = checkpoint.state.request_reset(GridResetReason::Manual)?;
    }
    Ok(checkpoint)
}

fn save_checkpoint(
    store: &ProjectionStore,
    checkpoint: &Stage7GridCheckpoint,
) -> Result<(), Stage7GridError> {
    store.save(checkpoint)?;
    stage7_inventory_recovery_evidence::capture_stage7_checkpoint(store.path(), checkpoint)
}

fn order_health_due(checkpoint: &Stage7GridCheckpoint, now_ms: u64) -> bool {
    checkpoint.last_order_health_checked_at_ms == 0
        || now_ms.saturating_sub(checkpoint.last_order_health_checked_at_ms)
            >= ORDER_HEALTH_INTERVAL_MS
}

#[allow(clippy::too_many_arguments)]
fn persist_order_health(
    artifacts_root: &Path,
    checkpoint_store: &ProjectionStore,
    checkpoint: &mut Stage7GridCheckpoint,
    commands: &CommandJournal,
    readback: &GridVenueReadback,
    generation: u64,
    now_ms: u64,
    transitioning: bool,
    force_order_health_check: &mut bool,
    exchange: &str,
) -> Result<(), Stage7GridError> {
    let report = stage7_health::evaluate(checkpoint, commands, readback, generation, now_ms);
    let report = if transitioning {
        stage7_health::transitioning_after_dispatch(report)
    } else {
        report
    };
    stage7_health::persist(artifacts_root, &report)?;
    info!(
        event = "stage7_grid_order_health",
        exchange,
        status = ?report.status,
        observed_orders = report.observed_orders,
        expected_orders = report.expected_orders,
        "阶段7订单健康检查已完成"
    );

    if report.status == stage7_health::Stage7GridHealthStatus::Transitioning {
        // A transitional snapshot is useful evidence but not a completed 30-minute check. Keep
        // the prior watermark so the next stable signed generation must be evaluated immediately.
        *force_order_health_check = true;
        return Ok(());
    }

    checkpoint.last_order_health_checked_at_ms = now_ms;
    *force_order_health_check = false;
    if report.is_unhealthy() {
        checkpoint.order_health_fenced = true;
        save_checkpoint(checkpoint_store, checkpoint)?;
        return Err(Stage7GridError::OrderHealth);
    }
    save_checkpoint(checkpoint_store, checkpoint)
}

fn record_readback(
    evidence: &mut PrivateEvidenceJournal,
    checkpoint: &Stage7GridCheckpoint,
    now_ms: u64,
    readback: &GridVenueReadback,
) -> Result<u64, Stage7GridError> {
    if readback.raw_private_payloads.is_empty() {
        return Err(Stage7GridError::PrivateEvidence);
    }
    let recovered_generation = evidence.last_generation();
    let generation = checkpoint
        .private_generation
        .max(recovered_generation)
        .checked_add(1)
        .ok_or(Stage7GridError::Clock)?;
    for payload in &readback.raw_private_payloads {
        append_private_payload(evidence, generation, now_ms, payload.clone())?;
    }
    Ok(generation)
}

fn advance_bitget_fill_history_window<V: HedgedGridVenue>(
    checkpoint: &mut Stage7GridCheckpoint,
    venue: &mut V,
    now_ms: u64,
) -> bool {
    let Some(next_start_ms) =
        next_fill_history_start_ms(venue.exchange(), checkpoint.fill_history_start_ms, now_ms)
    else {
        return false;
    };
    checkpoint.fill_history_start_ms = next_start_ms;
    venue.set_fill_history_start_ms(next_start_ms);
    true
}

fn next_fill_history_start_ms(exchange: &str, current_start_ms: u64, now_ms: u64) -> Option<u64> {
    if exchange != "bitget" {
        return None;
    }
    let candidate = now_ms.saturating_sub(BITGET_FILL_HISTORY_OVERLAP_MS);
    (candidate > current_start_ms).then_some(candidate)
}

fn append_private_payload(
    evidence: &mut PrivateEvidenceJournal,
    generation: u64,
    now_ms: u64,
    payload: String,
) -> Result<(), Stage7GridError> {
    evidence.append(PrivateEvidence::new(generation, now_ms, payload)?)?;
    Ok(())
}

fn event_payload(event: &GridPrivateEvent) -> String {
    match event {
        GridPrivateEvent::Fill { raw_payload, .. }
        | GridPrivateEvent::Reconcile { raw_payload } => raw_payload.clone(),
    }
}

#[path = "stage7_readback.rs"]
mod stage7_readback;
use stage7_readback::*;

fn active_writer(
    authority: &WriterLeaseAuthority,
    previous: Option<WriterSession>,
    now_ms: u64,
    generation: u64,
) -> Result<WriterSession, Stage7GridError> {
    match previous {
        Some(session) => match authority.renew(&session, now_ms) {
            Ok(session) => Ok(session),
            // The grid deliberately retains its exact writer identity while it waits for a
            // natural fill. A later signed readback has just advanced `generation`, so an
            // elapsed short lease may be reopened only through the authority's exact
            // same-scope recovery path; it must not be treated as a new writer election.
            Err(WriterLeaseError::Fenced | WriterLeaseError::Expired) => authority
                .recover_same_scope_after_readback(&session, generation, now_ms)
                .map_err(Into::into),
            Err(error) => Err(error.into()),
        },
        None => match authority.active_session()? {
            None => authority
                .register_initial(now_ms, generation)
                .map_err(Into::into),
            Some(session) => authority
                .recover_same_scope_after_readback(&session, generation, now_ms)
                .map_err(Into::into),
        },
    }
}

fn cancel_visible_owned_orders<V: HedgedGridVenue>(
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    state: &HedgedGridState,
    orders: &[Order],
) -> Result<bool, Stage7GridError> {
    let mut mutations = Vec::new();
    for order in orders {
        let key = validate_signed_checkpoint_order(order, state, commands, binding)?;
        mutations.push(Stage7Mutation::from_grid(next_cancel_command(
            commands, binding, &key,
        )?));
    }
    if mutations.is_empty() {
        return Ok(false);
    }
    execute_mutations(commands, venue, authority, writer, mutations, true)?;
    Ok(true)
}

/// Applies the deployment's physical exchange bounds without changing the shared reducer's
/// grid topology. The target notional remains 5 USDT; the quantity is only raised to satisfy a
/// documented minimum quantity or minimum notional at the worst opening price in this epoch.
fn stage7_epoch<V: HedgedGridVenue>(
    state: &HedgedGridState,
    venue: &V,
    bid: Price,
    ask: Price,
    minimum_epoch: u64,
) -> Result<GridEpoch, Stage7GridError> {
    let mut epoch = match &state.inventory_recovery {
        crate::strategy::hedged_grid::InventoryRecoveryState::Rebuilding {
            fill_id,
            fill_price,
        } => {
            let fallback = state
                .epoch
                .as_ref()
                .and_then(|epoch| epoch.passive_book_fallback.as_ref())
                .filter(|fallback| fallback.matches_fill(fill_id, *fill_price))
                .cloned();
            let mut epoch = super::hedged_grid::epoch_at_anchor(
                state,
                venue.instrument(),
                fallback
                    .as_ref()
                    .map_or(*fill_price, |fallback| fallback.anchor_price),
            )?;
            epoch.passive_book_fallback = fallback;
            epoch
        }
        crate::strategy::hedged_grid::InventoryRecoveryState::Inactive
        | crate::strategy::hedged_grid::InventoryRecoveryState::Deficient { .. }
        | crate::strategy::hedged_grid::InventoryRecoveryState::AwaitingNextOwnedFill { .. }
        | crate::strategy::hedged_grid::InventoryRecoveryState::ReanchorPending { .. } => {
            super::hedged_grid::epoch_at_midpoint(state, venue.instrument(), bid, ask)?
        }
    };
    finalize_stage7_epoch(state, venue, &mut epoch, minimum_epoch)?;
    Ok(epoch)
}

fn stage7_midpoint_epoch<V: HedgedGridVenue>(
    state: &HedgedGridState,
    venue: &V,
    bid: Price,
    ask: Price,
    minimum_epoch: u64,
) -> Result<GridEpoch, Stage7GridError> {
    let mut epoch = super::hedged_grid::epoch_at_midpoint(state, venue.instrument(), bid, ask)?;
    finalize_stage7_epoch(state, venue, &mut epoch, minimum_epoch)?;
    Ok(epoch)
}

fn finalize_stage7_epoch<V: HedgedGridVenue>(
    state: &HedgedGridState,
    venue: &V,
    epoch: &mut GridEpoch,
    minimum_epoch: u64,
) -> Result<(), Stage7GridError> {
    epoch.epoch = epoch.epoch.max(minimum_epoch);
    let outer_distance = epoch.step.value() * Decimal::from(state.params.grid_count);
    let lowest_opening_price = Price::new(epoch.anchor_price.value() - outer_distance)
        .map_err(|_| Stage7GridError::Notional)?;
    epoch.grid_quantity = stage7_quantity(
        venue,
        state.params.order_notional.value,
        epoch.anchor_price.value(),
        lowest_opening_price,
    )?;
    epoch.validate(state.params.grid_count)?;
    Ok(())
}

fn stage7_quantity<V: HedgedGridVenue>(
    venue: &V,
    target_notional: Decimal,
    reference_price: Decimal,
    minimum_price: Price,
) -> Result<Decimal, Stage7GridError> {
    if !target_notional.is_sign_positive()
        || target_notional.is_zero()
        || !reference_price.is_sign_positive()
    {
        return Err(Stage7GridError::Notional);
    }
    let minimum_by_notional = venue.instrument().minimum_notional.value / minimum_price.value();
    align_up(
        (target_notional / reference_price)
            .max(venue.minimum_quantity())
            .max(minimum_by_notional),
        venue.instrument().quantity_step,
    )
    .map_err(Into::into)
}

fn stage7_market_command<V: HedgedGridVenue>(
    binding: &HedgedGridBinding,
    replenishment: &crate::strategy::hedged_grid::GridReplenishment,
    venue: &V,
    inventory: &GridInventory,
    bid: Price,
    ask: Price,
) -> Result<Stage7Mutation, Stage7GridError> {
    let GridMutation::Market(mut command) =
        market_command(binding, replenishment, venue.instrument(), inventory)
            .map_err(Stage7GridError::Legacy)?
    else {
        return Err(Stage7GridError::Command);
    };
    let executable = match command.side {
        OrderSide::Buy => ask,
        OrderSide::Sell => bid,
    };
    let one_grid = stage7_quantity(
        venue,
        replenishment.target_notional.value / Decimal::from(3_u8),
        inventory.mark_price.value(),
        executable,
    )?;
    command.quantity = one_grid * Decimal::from(3_u8);
    Ok(Stage7Mutation::Market(command))
}

fn next_unused_grid_epoch(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
) -> Result<u64, Stage7GridError> {
    let highest = commands
        .commands()
        .filter(|command| {
            command.owner().is_some_and(|owner| {
                owner.strategy_instance_id == binding.strategy_instance_id
                    && owner.run_id == binding.run_id
                    && owner.exchange == binding.exchange
                    && owner.account == binding.account
                    && owner.symbol == binding.symbol
            })
        })
        .filter_map(|command| command.native_client_id())
        .filter_map(|client_id| parse_grid_client_order_id(client_id.as_str()).ok())
        .map(|key| key.epoch)
        .max()
        .unwrap_or(0);
    highest.checked_add(1).ok_or(Stage7GridError::Command)
}

fn checkpoint_projection_is_wal_bound(
    state: &HedgedGridState,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    instrument: &crate::domain::Instrument,
) -> Result<bool, Stage7GridError> {
    for intent in state.owned_orders.values() {
        let GridMutation::Place(expected) = place_command(binding, instrument, intent)? else {
            return Err(Stage7GridError::Command);
        };
        let Some(actual) = commands.place_by_client_id(&expected.client_order_id) else {
            return Ok(false);
        };
        if actual != &expected {
            return Ok(false);
        }
    }
    Ok(true)
}

fn assert_order_notional(
    quantity: Decimal,
    price: Price,
    instrument: &crate::domain::Instrument,
) -> Result<(), Stage7GridError> {
    assert_order_notional_with_max(quantity, price, instrument, SINGLE_ORDER_MAX_NOTIONAL)
}

fn assert_grid_order_notional(
    quantity: Decimal,
    price: Price,
    instrument: &crate::domain::Instrument,
    binding: &HedgedGridBinding,
    grid_count: u8,
) -> Result<(), Stage7GridError> {
    let maximum = if binding.exchange == "binance"
        && binding.config_version == "shared-grid-v1"
        && grid_count == 10
    {
        BINANCE_LEGACY_GRID_ORDER_MAX_NOTIONAL
    } else {
        SINGLE_ORDER_MAX_NOTIONAL
    };
    assert_order_notional_with_max(quantity, price, instrument, maximum)
}

fn assert_order_notional_with_max(
    quantity: Decimal,
    price: Price,
    instrument: &crate::domain::Instrument,
    maximum: Decimal,
) -> Result<(), Stage7GridError> {
    let value = physical_notional(quantity, price);
    if value < instrument.minimum_notional.value || value > maximum {
        return Err(Stage7GridError::OrderNotional {
            quantity,
            price: price.value(),
            value,
            minimum: instrument.minimum_notional.value,
            maximum,
        });
    }
    Ok(())
}

fn assert_market_notional(
    mutation: &Stage7Mutation,
    bid: Price,
    ask: Price,
    instrument: &crate::domain::Instrument,
) -> Result<(), Stage7GridError> {
    let Stage7Mutation::Market(command) = mutation else {
        return Err(Stage7GridError::Command);
    };
    let executable = match command.side {
        OrderSide::Buy => ask,
        OrderSide::Sell => bid,
    };
    let value = physical_notional(command.quantity, executable);
    if value < instrument.minimum_notional.value || value > REPLENISHMENT_MAX_NOTIONAL {
        return Err(Stage7GridError::MarketNotional {
            quantity: command.quantity,
            price: executable.value(),
            value,
            minimum: instrument.minimum_notional.value,
        });
    }
    Ok(())
}

/// Basic Hedge/Reduce Canary opens exactly one grid unit, so it uses the per-order 6 USDT cap.
/// The wider 18 USDT market cap belongs only to the unchanged three-grid replenishment wave.
fn assert_single_market_notional(
    mutation: &Stage7Mutation,
    bid: Price,
    ask: Price,
    instrument: &crate::domain::Instrument,
) -> Result<(), Stage7GridError> {
    let Stage7Mutation::Market(command) = mutation else {
        return Err(Stage7GridError::Command);
    };
    let executable = match command.side {
        OrderSide::Buy => ask,
        OrderSide::Sell => bid,
    };
    assert_order_notional(command.quantity, executable, instrument)
}

fn execute_mutations<V: HedgedGridVenue>(
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    mutations: Vec<Stage7Mutation>,
    prepare: bool,
) -> Result<(), Stage7GridError> {
    if mutations.is_empty() {
        return Ok(());
    }
    if commands.has_unresolved() && prepare {
        return Err(Stage7GridError::Unresolved);
    }
    for mutation in &mutations {
        if venue
            .validate_client_order_id(mutation.client_order_id())
            .is_err()
        {
            return Err(Stage7GridError::Rejected);
        }
    }
    if prepare {
        commands.prepare_submitted_batch(
            mutations
                .iter()
                .map(Stage7Mutation::execution_command)
                .collect(),
        )?;
    } else {
        for mutation in &mutations {
            commands.transition(mutation.command_id(), CommandState::Submitted)?;
        }
    }
    let client = venue.mutation_client();
    let _guard = authority.persistent_dispatch_guard(writer)?;
    let outcomes = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(mutations.len());
        for mutation in &mutations {
            let client = client.clone();
            handles.push(scope.spawn(move || mutation.submit(client.as_ref())));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().map_err(|_| Stage7GridError::Dispatch))
            .collect::<Result<Vec<_>, _>>()
    })?;
    for (mutation, outcome) in mutations.into_iter().zip(outcomes) {
        match outcome {
            Ok(venue_order_id) => {
                commands.transition(
                    mutation.command_id(),
                    CommandState::Accepted { venue_order_id },
                )?;
            }
            Err(mut error) => {
                // Bitget can accept a request while omitting the order id from its immediate
                // response. Recover only the same durable client identity; an incomplete exact
                // query remains WAL-pending for the next signed readback rather than becoming a
                // fabricated rejection or a second submission.
                let submitted_client_order_id = match &mutation {
                    Stage7Mutation::Place(command) => Some(command.client_order_id.as_str()),
                    Stage7Mutation::Market(command) => Some(command.client_order_id.as_str()),
                    Stage7Mutation::Reduce(command) => Some(command.client_order_id.as_str()),
                    Stage7Mutation::Cancel(_) => None,
                };
                if let Some(client_order_id) = submitted_client_order_id
                    && let Ok(ExactOrderRecovery::Found(venue_order_id)) =
                        recover_order(client_order_id, venue)
                {
                    commands.transition(
                        mutation.command_id(),
                        CommandState::Accepted { venue_order_id },
                    )?;
                    continue;
                }
                // A cancel can cross an exchange-side fill/cancel boundary.  Only an exact
                // client-identity query may settle that race; an active or unqueryable target
                // keeps the existing rejected/unknown fail-closed handling below.
                if let Stage7Mutation::Cancel(command) = &mutation {
                    match recover_cancel(command.target_client_order_id.as_str(), venue) {
                        Ok(Some(venue_order_id)) => {
                            commands.transition(
                                mutation.command_id(),
                                CommandState::Accepted { venue_order_id },
                            )?;
                            continue;
                        }
                        Ok(None) if is_rejected(&error) => {
                            // Bitget documents that a successful cancel acknowledgement is not
                            // terminal and advises retrying when an exact query still reports an
                            // active order. Reuse this same durable cancel identity once; no new
                            // mutation or strategy decision is created.
                            thread::sleep(Duration::from_millis(REJECTED_CANCEL_RETRY_MS));
                            match mutation.submit(client.as_ref()) {
                                Ok(venue_order_id) => {
                                    commands.transition(
                                        mutation.command_id(),
                                        CommandState::Accepted { venue_order_id },
                                    )?;
                                    continue;
                                }
                                Err(retry_error) => {
                                    error = retry_error;
                                    if let Ok(Some(venue_order_id)) = recover_cancel(
                                        command.target_client_order_id.as_str(),
                                        venue,
                                    ) {
                                        commands.transition(
                                            mutation.command_id(),
                                            CommandState::Accepted { venue_order_id },
                                        )?;
                                        continue;
                                    }
                                }
                            }
                        }
                        Ok(None) | Err(_) => {}
                    }
                }
                if is_rejected(&error) {
                    commands.transition(
                        mutation.command_id(),
                        CommandState::Rejected {
                            reason: error.to_string(),
                        },
                    )?;
                    return Err(Stage7GridError::Rejected);
                }
                commands.transition(
                    mutation.command_id(),
                    CommandState::Unknown {
                        reason: error.to_string(),
                    },
                )?;
                return Err(Stage7GridError::Unresolved);
            }
        }
    }
    Ok(())
}

fn is_rejected(error: &GridVenueError) -> bool {
    matches!(
        error,
        GridVenueError::BinancePrivate(
            crate::exchange::binance::PrivateError::Rejected { .. }
                | crate::exchange::binance::PrivateError::ClientOrderId
                | crate::exchange::binance::PrivateError::PostOnlyVerification,
        ) | GridVenueError::Gate(
            crate::exchange::gate::GateError::Rejected { .. }
                | crate::exchange::gate::GateError::ClientOrderId,
        ) | GridVenueError::Bitget(
            crate::exchange::bitget::BitgetError::Rejected
                | crate::exchange::bitget::BitgetError::RejectedCode { .. }
                | crate::exchange::bitget::BitgetError::RejectedHttp { .. }
                | crate::exchange::bitget::BitgetError::ClientOrderId
        )
    )
}

fn is_order_absent(error: &GridVenueError) -> bool {
    matches!(
        error,
        GridVenueError::BinancePrivate(crate::exchange::binance::PrivateError::Rejected {
            api_code: Some(-2013),
            ..
        },) | GridVenueError::Gate(crate::exchange::gate::GateError::OrderAbsent)
            | GridVenueError::Bitget(crate::exchange::bitget::BitgetError::OrderAbsent)
    )
}

fn recover_unresolved<V: HedgedGridVenue>(
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
    allow_prepared_risk_increase: bool,
) -> Result<(), Stage7GridError> {
    let unresolved = commands.unresolved_command_ids();
    let mut prepared_risk_reducing = Vec::new();
    let mut prepared_risk_increasing = Vec::new();
    for command_id in unresolved {
        let receipt = commands
            .receipt(&command_id)
            .cloned()
            .ok_or(Stage7GridError::JournalScope)?;
        match &receipt.command {
            ExecutionCommand::Cancel(command) => {
                let owner = commands
                    .owner_by_client_id(&command.target_client_order_id)
                    .ok_or(Stage7GridError::JournalScope)?;
                validate_owner_binding(owner, binding)?;
            }
            command => validate_command_binding(command, binding)?,
        }
        if matches!(receipt.state, CommandState::Prepared) {
            let mutation = Stage7Mutation::from_execution(receipt.command)?;
            if matches!(mutation, Stage7Mutation::Reduce(_)) {
                commands.transition(
                    &command_id,
                    CommandState::Rejected {
                        reason: "prepared_exposure_reduction_requires_fresh_generation".to_owned(),
                    },
                )?;
                continue;
            }
            let increases_risk = matches!(
                &mutation,
                Stage7Mutation::Place(command) if !command.reduce_only
            ) || matches!(
                &mutation,
                Stage7Mutation::Market(command) if !command.reduce_only
            );
            if increases_risk && !allow_prepared_risk_increase {
                // Prepared proves the network call never began. When current rules are not
                // proven (or a stop is active), terminate that dormant opening identity instead
                // of reviving risk from an old WAL after restart.
                commands.transition(
                    &command_id,
                    CommandState::Rejected {
                        reason: "prepared_risk_rejected_without_current_instrument_rules"
                            .to_owned(),
                    },
                )?;
            } else {
                if increases_risk {
                    prepared_risk_increasing.push(mutation);
                } else {
                    prepared_risk_reducing.push(mutation);
                }
            }
            continue;
        }
        if legacy_gate_flatten_identity_never_dispatched(&receipt, binding, venue) {
            commands.transition(
                &command_id,
                CommandState::Rejected {
                    reason: "legacy_gate_flatten_client_id_proved_never_dispatched".to_owned(),
                },
            )?;
            continue;
        }
        let mutation = Stage7Mutation::from_execution(receipt.command)?;
        let mut exact_query_incomplete = false;
        let resolved = match &mutation {
            Stage7Mutation::Place(command) => {
                match recover_order(command.client_order_id.as_str(), venue)? {
                    ExactOrderRecovery::Found(venue_order_id) => Some(venue_order_id),
                    ExactOrderRecovery::Absent => {
                        exact_query_incomplete = true;
                        None
                    }
                    ExactOrderRecovery::Incomplete => {
                        exact_query_incomplete = true;
                        None
                    }
                }
            }
            Stage7Mutation::Market(command) => {
                match recover_order(command.client_order_id.as_str(), venue)? {
                    ExactOrderRecovery::Found(venue_order_id) => Some(venue_order_id),
                    ExactOrderRecovery::Absent => {
                        exact_query_incomplete = true;
                        None
                    }
                    ExactOrderRecovery::Incomplete => {
                        exact_query_incomplete = true;
                        None
                    }
                }
            }
            Stage7Mutation::Reduce(command) => {
                match recover_order(command.client_order_id.as_str(), venue)? {
                    ExactOrderRecovery::Found(venue_order_id) => Some(venue_order_id),
                    ExactOrderRecovery::Absent | ExactOrderRecovery::Incomplete => {
                        match exact_market_reduce_fill_recovery(command, readback) {
                            ExactMarketReduceFillRecovery::Proven { venue_order_id, .. } => {
                                Some(venue_order_id)
                            }
                            ExactMarketReduceFillRecovery::Missing
                            | ExactMarketReduceFillRecovery::Conflicting => {
                                exact_query_incomplete = true;
                                None
                            }
                        }
                    }
                }
            }
            Stage7Mutation::Cancel(command) => {
                if signed_open_order_is_absent(readback, command.target_client_order_id.as_str()) {
                    // The signed open-orders surface is complete for this symbol.  A cancel's
                    // semantic target is the removal of that active identity, so its absence
                    // resolves a racing/unknown request without trusting a brittle historical
                    // single-order payload. Inventory is still reconciled immediately after.
                    Some("absent_in_signed_open_orders".to_owned())
                } else {
                    recover_cancel(command.target_client_order_id.as_str(), venue)?
                }
            }
        };
        match resolved {
            Some(venue_order_id) => {
                commands.transition(&command_id, CommandState::Accepted { venue_order_id })?;
            }
            None if exact_query_incomplete => {}
            None => {}
        }
    }
    if !prepared_risk_reducing.is_empty() {
        // A WAL record can survive a process exit before the network call. It is safe to submit
        // the same durable client identity only; a new identity is never generated in recovery.
        execute_mutations(
            commands,
            venue,
            authority,
            writer,
            prepared_risk_reducing,
            false,
        )?;
    }
    // A closing/cancel request that had crossed the dispatch boundary before the crash remains an
    // execution debt. Do not revive any prepared opening until its exact identity is terminal.
    if commands.unresolved_command_ids().iter().any(|command_id| {
        commands
            .receipt(command_id)
            .is_some_and(|receipt| match &receipt.command {
                ExecutionCommand::Cancel(_) | ExecutionCommand::MarketReduce(_) => true,
                ExecutionCommand::PlaceLimit(command) => command.reduce_only,
                ExecutionCommand::PlaceMarket(command) => command.reduce_only,
                ExecutionCommand::StopMarketCloseAll(_)
                | ExecutionCommand::StopMarketFullPosition(_) => true,
            })
    }) {
        return Ok(());
    }
    if !prepared_risk_increasing.is_empty() {
        execute_mutations(
            commands,
            venue,
            authority,
            writer,
            prepared_risk_increasing,
            false,
        )?;
    }
    Ok(())
}

fn legacy_gate_flatten_identity_never_dispatched<V: HedgedGridVenue>(
    receipt: &crate::execution::CommandReceipt,
    binding: &HedgedGridBinding,
    venue: &V,
) -> bool {
    if binding.exchange != "gate" {
        return false;
    }
    let CommandState::Unknown { reason } = &receipt.state else {
        return false;
    };
    if reason != &crate::exchange::gate::GateError::ClientOrderId.to_string() {
        return false;
    }
    let ExecutionCommand::PlaceLimit(command) = &receipt.command else {
        return false;
    };
    let client_order_id = command.client_order_id.as_str();
    let legacy_suffix = client_order_id.strip_prefix("hgf_m_");
    command.owner.purpose == OrderPurpose::Reduce
        && command.reduce_only
        && client_order_id.len() == 30
        && legacy_suffix.is_some_and(|suffix| {
            suffix.len() == 24
                && suffix
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        })
        && !crate::exchange::gate::client_order_id_is_valid(client_order_id)
        && venue.proves_never_dispatched(&receipt.command, reason)
}

fn signed_open_order_is_absent(readback: &GridVenueReadback, client_order_id: &str) -> bool {
    !readback.orders.iter().any(|order| {
        matches!(order.state, OrderState::New | OrderState::PartiallyFilled)
            && matches!(&order.client_order_id, FieldState::Known(value) if value == client_order_id)
    })
}

fn recover_order<V: HedgedGridVenue>(
    client_order_id: &str,
    venue: &mut V,
) -> Result<ExactOrderRecovery, Stage7GridError> {
    match venue.order_by_client_id(client_order_id) {
        Ok(order)
            if matches!(
                order.state,
                OrderState::New
                    | OrderState::PartiallyFilled
                    | OrderState::Filled
                    | OrderState::Cancelled
                    | OrderState::Expired
            ) =>
        {
            Ok(ExactOrderRecovery::Found(order.order_id))
        }
        Ok(_) => Ok(ExactOrderRecovery::Absent),
        Err(error) if is_order_absent(&error) => Ok(ExactOrderRecovery::Absent),
        Err(error) if incomplete_exact_order_query(&error) => Ok(ExactOrderRecovery::Incomplete),
        Err(error) => Err(error.into()),
    }
}

enum ExactOrderRecovery {
    Found(String),
    Absent,
    Incomplete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ExactMarketReduceFillRecovery {
    Proven {
        venue_order_id: String,
        cumulative_quantity: Decimal,
    },
    Missing,
    Conflicting,
}

/// A complete signed fill surface may settle a purged market order only when every durable
/// identity and execution fact still proves the one WAL-bound exposure episode. Ambiguous facts
/// deliberately leave the command UNKNOWN so recovery cannot authorize a second reduction.
fn exact_market_reduce_fill_recovery(
    command: &crate::domain::MarketReduceCommand,
    readback: &GridVenueReadback,
) -> ExactMarketReduceFillRecovery {
    if readback.raw_private_payloads.is_empty()
        || command.validate().is_err()
        || command.owner.purpose != OrderPurpose::ExposureTakeProfit
        || !signed_open_order_is_absent(readback, command.client_order_id.as_str())
    {
        return ExactMarketReduceFillRecovery::Conflicting;
    }

    let mut unique_fills = BTreeMap::<&str, &GridVenueFill>::new();
    for record in &readback.fills {
        match unique_fills.get(record.fill.fill_id.as_str()) {
            Some(previous) if **previous != *record => {
                return ExactMarketReduceFillRecovery::Conflicting;
            }
            Some(_) => {}
            None => {
                unique_fills.insert(record.fill.fill_id.as_str(), record);
            }
        }
    }

    let mut venue_order_id: Option<&str> = None;
    let mut sequences = BTreeSet::new();
    let mut cumulative_quantity = Decimal::ZERO;
    for record in unique_fills.values().copied().filter(|record| {
        matches!(
            &record.client_order_id,
            FieldState::Known(client_id) if client_id == command.client_order_id.as_str()
        )
    }) {
        let fill = &record.fill;
        let FieldState::Known(sequence) = fill.execution_sequence else {
            return ExactMarketReduceFillRecovery::Conflicting;
        };
        if fill.validate().is_err()
            || fill.symbol != command.owner.symbol
            || fill.side != command.side
            || fill.position_side != FieldState::Known(command.position_side)
            || fill.maker != FieldState::Known(false)
            || !sequences.insert(sequence)
            || venue_order_id.is_some_and(|order_id| order_id != fill.order_id)
        {
            return ExactMarketReduceFillRecovery::Conflicting;
        }
        venue_order_id = Some(fill.order_id.as_str());
        let Some(total) = cumulative_quantity.checked_add(fill.quantity) else {
            return ExactMarketReduceFillRecovery::Conflicting;
        };
        if total > command.quantity {
            return ExactMarketReduceFillRecovery::Conflicting;
        }
        cumulative_quantity = total;
    }

    let Some(venue_order_id) = venue_order_id else {
        return ExactMarketReduceFillRecovery::Missing;
    };
    if readback.fills.iter().any(|record| {
        record.fill.order_id == venue_order_id
            && !matches!(
                &record.client_order_id,
                FieldState::Known(client_id) if client_id == command.client_order_id.as_str()
            )
    }) {
        return ExactMarketReduceFillRecovery::Conflicting;
    }

    ExactMarketReduceFillRecovery::Proven {
        venue_order_id: venue_order_id.to_owned(),
        cumulative_quantity,
    }
}

fn recover_cancel<V: HedgedGridVenue>(
    target_client_order_id: &str,
    venue: &mut V,
) -> Result<Option<String>, Stage7GridError> {
    match venue.order_by_client_id(target_client_order_id) {
        Ok(order)
            if matches!(
                order.state,
                OrderState::Cancelled
                    | OrderState::Filled
                    | OrderState::Expired
                    | OrderState::Rejected
            ) =>
        {
            Ok(Some(order.order_id))
        }
        Ok(_) => Ok(None),
        Err(error) if is_order_absent(&error) => {
            Ok(Some("absent_after_signed_readback".to_owned()))
        }
        Err(error) if incomplete_exact_order_query(&error) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn incomplete_exact_order_query(error: &GridVenueError) -> bool {
    matches!(
        error,
        GridVenueError::Bitget(
            crate::exchange::bitget::BitgetError::Payload
                | crate::exchange::bitget::BitgetError::Readback(_)
                | crate::exchange::bitget::BitgetError::Http
                | crate::exchange::bitget::BitgetError::RateLimited
                | crate::exchange::bitget::BitgetError::Rejected
                | crate::exchange::bitget::BitgetError::RejectedCode { .. }
                | crate::exchange::bitget::BitgetError::RejectedHttp { .. }
        )
    )
}

#[path = "stage7_fill_drive.rs"]
mod stage7_fill_drive;
use stage7_fill_drive::*;

fn reconcile_signed_order_loss(
    state: &mut HedgedGridState,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
) -> Result<Option<(usize, usize)>, Stage7GridError> {
    if state.phase != GridPhase::Running
        || signed_desired_ladder_is_complete(state, commands, binding, readback)?
    {
        return Ok(None);
    }
    let expected_orders = state.owned_orders.len();
    let owned = recovered_owned_orders(commands, binding, readback)?;
    let observed_orders = owned.len();
    state.begin_reconciliation_reset(owned)?;
    Ok(Some((expected_orders, observed_orders)))
}

pub(in crate::runtime) fn resolve_grid_fill_client_ids(
    commands: &CommandJournal,
    fills: &[GridVenueFill],
) -> Vec<GridVenueFill> {
    fills
        .iter()
        .map(|record| {
            if matches!(record.client_order_id, FieldState::Known(_)) {
                return record.clone();
            }
            let Some(client_id) = commands.client_id_by_venue_order_id(&record.fill.order_id)
            else {
                return record.clone();
            };
            let mut resolved = record.clone();
            resolved.client_order_id = FieldState::Known(client_id.as_str().to_owned());
            resolved
        })
        .collect()
}

fn sort_grid_fill_candidates_by_execution_sequence(
    fills: &mut Vec<(&GridVenueFill, GridOrderKey)>,
) -> Result<(), Stage7GridError> {
    let mut account_sequences = BTreeMap::<u64, String>::new();
    for (record, _) in fills.iter() {
        let FieldState::Known(sequence) = record.fill.execution_sequence else {
            return Err(Stage7GridError::FillLiquidityUnknown);
        };
        match account_sequences.get(&sequence) {
            None => {
                account_sequences.insert(sequence, record.fill.fill_id.clone());
            }
            Some(existing) if existing == &record.fill.fill_id => {}
            Some(_) => return Err(Stage7GridError::FillLiquidityUnknown),
        }
    }
    fills.sort_by_key(|(record, _)| match record.fill.execution_sequence {
        FieldState::Known(sequence) => sequence,
        _ => u64::MAX,
    });
    Ok(())
}

#[cfg(test)]
fn signed_full_owned_fill(record: &GridVenueFill, expected_quantity: Decimal) -> bool {
    record.fill.quantity == expected_quantity
}

fn signed_owned_fill_quantities(fills: &[GridVenueFill]) -> BTreeMap<GridOrderKey, Decimal> {
    // Bitget reports executions, not one terminal row per order. Deduplicate execution ids,
    // then aggregate every signed execution for the exact owned client identity so that a
    // 29+26 split can prove the same 55-unit completion as one 55-unit execution. Conflicting
    // reuse of an execution id proves nothing and is excluded from both possible identities.
    let mut executions = BTreeMap::<String, Option<(GridOrderKey, Decimal)>>::new();
    for record in fills {
        let FieldState::Known(client_order_id) = &record.client_order_id else {
            continue;
        };
        let Ok(key) = parse_grid_client_order_id(client_order_id) else {
            continue;
        };
        if !matches!(
            super::hedged_grid::route_grid_fill(&record.fill),
            super::hedged_grid::GridFillRoute::MakerDrive
        ) {
            continue;
        }
        let candidate = (key, record.fill.quantity);
        match executions.get_mut(&record.fill.fill_id) {
            None => {
                executions.insert(record.fill.fill_id.clone(), Some(candidate));
            }
            Some(existing) if existing.as_ref() == Some(&candidate) => {}
            Some(existing) => *existing = None,
        }
    }

    let mut quantities = BTreeMap::<GridOrderKey, Decimal>::new();
    for (key, quantity) in executions.into_values().flatten() {
        let Some(total) = quantities
            .get(&key)
            .copied()
            .unwrap_or(Decimal::ZERO)
            .checked_add(quantity)
        else {
            quantities.remove(&key);
            continue;
        };
        quantities.insert(key, total);
    }
    quantities
}

fn signed_complete_owned_fill_present_resolved(
    owned_orders: &std::collections::BTreeMap<
        crate::strategy::hedged_grid::GridOrderKey,
        crate::strategy::hedged_grid::GridOrderIntent,
    >,
    commands: &CommandJournal,
    fills: &[GridVenueFill],
) -> bool {
    let resolved = resolve_grid_fill_client_ids(commands, fills);
    signed_complete_owned_fill_present(owned_orders, &resolved)
}

fn signed_complete_owned_fill_present(
    owned_orders: &std::collections::BTreeMap<
        crate::strategy::hedged_grid::GridOrderKey,
        crate::strategy::hedged_grid::GridOrderIntent,
    >,
    fills: &[GridVenueFill],
) -> bool {
    signed_owned_fill_quantities(fills)
        .iter()
        .any(|(key, quantity)| {
            owned_orders
                .get(key)
                .is_some_and(|order| order.quantity == *quantity)
        })
}

fn canary_observed_owned_execution(
    phase_before_inventory: GridPhase,
    actions: &[GridAction],
) -> bool {
    phase_before_inventory == GridPhase::Running
        && !actions.is_empty()
        && actions.iter().all(|action| {
            matches!(
                action,
                GridAction::Reset {
                    reason: GridResetReason::InventoryLow
                }
            )
        })
}

fn rolling_actions_exceed_order_cap(
    actions: &[GridAction],
    instrument: &crate::domain::Instrument,
    binding: &HedgedGridBinding,
    grid_count: u8,
) -> Result<bool, Stage7GridError> {
    for action in actions {
        let GridAction::Dispatch(transaction) = action else {
            continue;
        };
        for intent in &transaction.places {
            match assert_grid_order_notional(
                intent.quantity,
                intent.price,
                instrument,
                binding,
                grid_count,
            ) {
                Ok(()) => {}
                Err(Stage7GridError::OrderNotional { .. }) => return Ok(true),
                Err(error) => return Err(error),
            }
        }
    }
    Ok(false)
}

fn rolling_transaction_ids(actions: &[GridAction]) -> Vec<String> {
    actions
        .iter()
        .filter_map(|action| match action {
            GridAction::Dispatch(transaction) => Some(transaction.id.clone()),
            GridAction::Reset { .. }
            | GridAction::Place(_)
            | GridAction::Replenish(_)
            | GridAction::ReanchorAtFill { .. } => None,
        })
        .collect()
}

fn request_reconciliation_reset_unless_batch_is_blocked(
    state: &mut HedgedGridState,
) -> Result<(), HedgedGridError> {
    // A rejected or unresolved rolling batch has already installed a durable delayed
    // reconciliation fence. Preserve it: requesting another reset from BlockedUnknown is an
    // invalid transition and would terminate the resident before signed facts can rebuild it.
    if state.phase != GridPhase::BlockedUnknown {
        let _ = state.request_reset(GridResetReason::Reconciliation)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn dispatch_fill_actions<V>(
    checkpoint: &mut Stage7GridCheckpoint,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    actions: Vec<GridAction>,
    store: &ProjectionStore,
) -> Result<FillDriveOutcome, Stage7GridError>
where
    V: HedgedGridVenue,
{
    let transactions = actions
        .into_iter()
        .filter_map(|action| match action {
            GridAction::Dispatch(transaction) => Some(transaction),
            GridAction::Reset { .. }
            | GridAction::Place(_)
            | GridAction::Replenish(_)
            | GridAction::ReanchorAtFill { .. } => None,
        })
        .collect::<Vec<_>>();
    if transactions.is_empty() {
        return Ok(FillDriveOutcome::idle());
    }
    let mut mutations = Vec::new();
    for transaction in &transactions {
        for intent in &transaction.places {
            let mutation =
                Stage7Mutation::from_grid(place_command(binding, venue.instrument(), intent)?);
            let Stage7Mutation::Place(command) = &mutation else {
                return Err(Stage7GridError::Command);
            };
            assert_grid_order_notional(
                command.quantity,
                command.limit_price,
                venue.instrument(),
                binding,
                checkpoint.state.params.grid_count,
            )?;
            mutations.push(mutation);
        }
        mutations.push(Stage7Mutation::from_grid(next_cancel_command(
            commands,
            binding,
            &transaction.cancel,
        )?));
    }

    // Rolling intent is already fixed by the fill reducer. Dispatch it directly: every adapter
    // encodes the replacement as exchange-native post-only, which is the authoritative race
    // fence. A crossing replacement is rejected, journaled, and reconciled; it is never repriced,
    // converted to taker, or retried under the old command identity.
    let result = execute_mutations(commands, venue, authority, writer, mutations, true);
    let failed_closed = matches!(
        &result,
        Err(Stage7GridError::Unresolved | Stage7GridError::Rejected)
    );
    for transaction in transactions {
        checkpoint
            .state
            .settle_transaction(&transaction.id, result.is_ok())?;
    }
    if failed_closed {
        let not_before_ms = wall_clock_ms()?.saturating_add(REJECTED_GRID_RESET_DELAY_MS);
        checkpoint
            .state
            .defer_blocked_reconciliation_until(not_before_ms)?;
        warn!(
            event = "stage7_grid_rejected_reset_deferred",
            not_before_ms,
            delay_ms = REJECTED_GRID_RESET_DELAY_MS,
            "补撤请求未全部成功，保持新增风险关闭，30秒后按签名订单事实重建"
        );
    }
    save_checkpoint(store, checkpoint)?;
    match result {
        Ok(()) => Ok(FillDriveOutcome::dispatched()),
        Err(Stage7GridError::Unresolved | Stage7GridError::Rejected) => {
            Ok(FillDriveOutcome::failed_closed())
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
#[path = "stage7_exposure_composition_tests.rs"]
mod exposure_composition_tests;
#[cfg(test)]
#[path = "stage7_exposure_recovery_tests.rs"]
mod exposure_recovery_tests;
#[cfg(test)]
#[path = "stage7_exposure_shadow_verifier_tests.rs"]
mod exposure_shadow_verifier_tests;
#[cfg(test)]
#[path = "stage7_fill_sequence_tests.rs"]
mod fill_sequence_tests;
#[cfg(test)]
#[path = "stage7_install_recovery_tests.rs"]
mod install_recovery_tests;
#[cfg(test)]
#[path = "stage7_inventory_recovery_evidence_tests.rs"]
mod inventory_recovery_evidence_tests;
#[cfg(test)]
#[path = "hedged_grid_runtime_equivalence_tests.rs"]
mod runtime_equivalence_tests;
#[cfg(test)]
#[path = "stage7_grid_tests.rs"]
mod tests;
