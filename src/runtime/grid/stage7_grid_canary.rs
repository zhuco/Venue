use std::{thread, time::Duration};

use crate::{
    config::Config,
    domain::Symbol,
    exchange::grid::{BinanceGridVenue, BitgetGridVenue, GateGridVenue},
    execution::{CommandJournal, WRITER_LEASE_TTL_MS},
    storage::ProjectionStore,
    strategy::hedged_grid::{GridPhase, HedgedGridBinding},
};

use super::{
    CHECKPOINT_FILE, COMMAND_FILE, ORDER_HEALTH_FILE, Stage7CanaryRequest, Stage7GridCanaryReport,
    Stage7GridCheckpoint, Stage7GridError, Stage7GridRequest, binance_binding, bitget_binding,
    canary_cleanup_readback, canary_preflight, gate_binding, open_stage7_private_evidence,
    release_params, require_stage7_canary, run_stage7_canary_recovery, run_stage7_grid,
    set_stage7_grid_control,
};
use super::{
    stage7_canary_contract::Stage7CanaryVenue,
    stage7_canary_support::append_stage7_grid_lifecycle_capability,
    stage7_health::{Stage7GridHealthReport, Stage7GridHealthStatus},
};

const INSTALL_TURNS: u64 = 96;
const INSTALL_MAX_MS: u64 = 60 * 1_000;
// The low-balance lifecycle probe may wait for market execution, but it must never become an
// unbounded resident that can keep replenishing a small account.
const FILL_WAIT_TURNS: u64 = 1_800;
const FILL_WAIT_MAX_MS: u64 = 3 * 60 * 1_000;
const VERIFY_TURNS: u64 = 96;
const VERIFY_MAX_MS: u64 = 60 * 1_000;
const GRID_CANARY_VALIDITY_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

pub fn run_gate_stage7_grid_canary(
    cfg: &Config,
    request: Stage7CanaryRequest,
) -> Result<Stage7GridCanaryReport, Stage7GridError> {
    let binding = gate_binding(cfg)?;
    let _ = release_params(cfg, &binding)?;
    run(cfg, request, binding, "ggc", |symbol| {
        GateGridVenue::production(symbol, 1).map_err(Into::into)
    })
}

pub fn run_bitget_stage7_grid_canary(
    cfg: &Config,
    request: Stage7CanaryRequest,
) -> Result<Stage7GridCanaryReport, Stage7GridError> {
    let binding = bitget_binding(cfg)?;
    let _ = release_params(cfg, &binding)?;
    run(cfg, request, binding, "bgc", |symbol| {
        BitgetGridVenue::production(symbol, 1).map_err(Into::into)
    })
}

pub fn run_binance_stage7_grid_canary(
    cfg: &Config,
    request: Stage7CanaryRequest,
) -> Result<Stage7GridCanaryReport, Stage7GridError> {
    let binding = binance_binding(cfg)?;
    let _ = release_params(cfg, &binding)?;
    run(cfg, request, binding, "bnc", |symbol| {
        BinanceGridVenue::production(symbol, 1).map_err(Into::into)
    })
}

/// Runs the real low-balance release twice from disk: the first process installs/replenishes the
/// grid, the second waits for a natural complete owned fill, and the third verifies the durable
/// restart state. It then cancels only owned orders and reduce-only flattens before granting Live
/// admission evidence. No synthetic fill or alternate strategy path is used.
fn run<V, F>(
    cfg: &Config,
    request: Stage7CanaryRequest,
    binding: HedgedGridBinding,
    prefix: &str,
    mut create_venue: F,
) -> Result<Stage7GridCanaryReport, Stage7GridError>
where
    V: Stage7CanaryVenue,
    F: FnMut(Symbol) -> Result<V, Stage7GridError>,
{
    if !request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }

    let mut initial = create_venue(binding.symbol.clone())?;
    initial.verify_current_instrument_rules()?;
    let parameter_release = release_params(cfg, &binding)?;
    require_stage7_canary(
        &initial,
        &binding,
        &parameter_release,
        &request.artifacts_root,
        super::wall_clock_ms()?,
    )?;
    // Lifecycle Canary may follow a dedicated flat retirement, whose durable control correctly
    // remains Stop. Prove a fresh signed flat/empty state first, then request the ordinary
    // symbol-scoped Reset path; never revive a stopped root from stale checkpoint assumptions.
    let mut private_generation = ProjectionStore::new(request.artifacts_root.join(CHECKPOINT_FILE))
        .load::<Stage7GridCheckpoint>()?
        .map(|checkpoint| checkpoint.private_generation)
        .unwrap_or(0);
    let mut evidence = open_stage7_private_evidence(&request.artifacts_root, &binding)?;
    let (preflight_readback, preflight_inventory) = canary_cleanup_readback(
        &mut initial,
        &mut evidence,
        &mut private_generation,
        &binding,
    )?;
    canary_preflight(&preflight_readback, &preflight_inventory)?;
    set_stage7_grid_control(
        cfg,
        &request.artifacts_root,
        crate::runtime::HedgedGridControlTarget::Reset,
    )?;
    let first = match run_stage7_grid(
        cfg,
        grid_request(
            &request,
            Some(INSTALL_TURNS),
            true,
            true,
            false,
            Some(phase_deadline(INSTALL_MAX_MS)?),
        ),
        binding.clone(),
        &mut initial,
    ) {
        Ok(report) => report,
        Err(error) => return fail_after_cleanup(error, &request, &binding, &mut initial, prefix),
    };
    if first.phase != GridPhase::Running && !first.first_owned_fill_observed {
        return fail_after_cleanup(
            Stage7GridError::Canary,
            &request,
            &binding,
            &mut initial,
            prefix,
        );
    }

    let fill = if first.first_owned_fill_observed {
        first
    } else {
        drop(initial);
        let mut resumed = create_venue(binding.symbol.clone())?;
        match run_stage7_grid(
            cfg,
            grid_request(
                &request,
                Some(FILL_WAIT_TURNS),
                true,
                false,
                false,
                Some(phase_deadline(FILL_WAIT_MAX_MS)?),
            ),
            binding.clone(),
            &mut resumed,
        ) {
            Ok(report) if report.first_owned_fill_observed => report,
            Ok(_) => {
                return fail_after_cleanup(
                    Stage7GridError::Canary,
                    &request,
                    &binding,
                    &mut resumed,
                    prefix,
                );
            }
            Err(error) => {
                return fail_after_cleanup(error, &request, &binding, &mut resumed, prefix);
            }
        }
    };
    if !fill.first_owned_fill_observed {
        return Err(Stage7GridError::Canary);
    }

    // This is an intentional process-boundary simulation. The lease authority allows the exact
    // successor only after the predecessor's short lease elapsed and a new signed readback
    // advances its private generation; taking it earlier would be a real concurrent-writer bug.
    thread::sleep(Duration::from_millis(
        WRITER_LEASE_TTL_MS.saturating_add(250),
    ));
    let mut verified = create_venue(binding.symbol.clone())?;
    let verification = match run_stage7_grid(
        cfg,
        grid_request(
            &request,
            Some(VERIFY_TURNS),
            false,
            false,
            true,
            Some(phase_deadline(VERIFY_MAX_MS)?),
        ),
        binding.clone(),
        &mut verified,
    ) {
        Ok(report) => report,
        Err(error) => return fail_after_cleanup(error, &request, &binding, &mut verified, prefix),
    };
    // A real fill may legitimately issue a new rolling command during verification. Reopening
    // the journal rejects conflicting command/client identities; only unresolved WAL state is
    // disqualifying here. The order-health projection below validates the resulting live grid.
    if verification.phase != GridPhase::Running || !command_journal_is_settled(&request)? {
        return fail_after_cleanup(
            Stage7GridError::Canary,
            &request,
            &binding,
            &mut verified,
            prefix,
        );
    }
    let health = ProjectionStore::new(request.artifacts_root.join(ORDER_HEALTH_FILE))
        .load::<Stage7GridHealthReport>()?
        .ok_or(Stage7GridError::Canary)?;
    if !healthy_configured_grid(&health, parameter_release.grid_count) {
        return fail_after_cleanup(
            Stage7GridError::Canary,
            &request,
            &binding,
            &mut verified,
            prefix,
        );
    }

    let cleanup =
        run_stage7_canary_recovery(request.clone(), binding.clone(), &mut verified, prefix)?;
    let valid_until_ms = super::wall_clock_ms()?.saturating_add(GRID_CANARY_VALIDITY_MS);
    let parameter_release = release_params(cfg, &binding)?;
    append_stage7_grid_lifecycle_capability(
        &verified.capability_binding(),
        &binding,
        &parameter_release,
        verified.instrument(),
        verified.minimum_quantity(),
        cfg.hedged_grid.and_then(|grid| grid.exposure_take_profit),
        &request.artifacts_root,
        cleanup.private_generation,
        health.private_generation,
        valid_until_ms,
    )?;
    Ok(Stage7GridCanaryReport {
        exchange: binding.exchange,
        symbol: binding.symbol.to_string(),
        private_generation: cleanup.private_generation,
        capability_valid_until_ms: valid_until_ms,
    })
}

fn grid_request(
    request: &Stage7CanaryRequest,
    max_turns: Option<u64>,
    stop_after_first_owned_fill: bool,
    reset_on_start: bool,
    force_order_health_check: bool,
    wall_clock_deadline_ms: Option<u64>,
) -> Stage7GridRequest {
    Stage7GridRequest {
        artifacts_root: request.artifacts_root.clone(),
        max_turns,
        // A lifecycle validation may follow a signed emergency cleanup from a prior attempt.
        // Rebuilding only its first resident converts that durable, flat checkpoint into a fresh
        // epoch; the fill wait and restart verification deliberately preserve the new state.
        reset_on_start,
        skip_inventory_replenishment_until_recovered: false,
        confirm_mainnet_grid_mutations: true,
        shadow_only: false,
        stop_after_first_owned_fill,
        wall_clock_deadline_ms,
        force_order_health_check,
    }
}

fn phase_deadline(max_elapsed_ms: u64) -> Result<u64, Stage7GridError> {
    super::wall_clock_ms()?
        .checked_add(max_elapsed_ms)
        .ok_or(Stage7GridError::Clock)
}

fn command_journal_is_settled(request: &Stage7CanaryRequest) -> Result<bool, Stage7GridError> {
    Ok(!CommandJournal::open(request.artifacts_root.join(COMMAND_FILE))?.has_unresolved())
}

fn healthy_configured_grid(report: &Stage7GridHealthReport, grid_count: u8) -> bool {
    report.status == Stage7GridHealthStatus::Healthy
        && report.expected_long_opening == grid_count
        && report.observed_long_opening == grid_count
        && report.expected_short_opening == grid_count
        && report.observed_short_opening == grid_count
        && report.expected_long_closing <= grid_count
        && report.observed_long_closing == report.expected_long_closing
        && report.expected_short_closing <= grid_count
        && report.observed_short_closing == report.expected_short_closing
        && !report.has_unresolved_wal
}

fn fail_after_cleanup<V: Stage7CanaryVenue>(
    error: Stage7GridError,
    request: &Stage7CanaryRequest,
    binding: &HedgedGridBinding,
    venue: &mut V,
    prefix: &str,
) -> Result<Stage7GridCanaryReport, Stage7GridError> {
    match run_stage7_canary_recovery(request.clone(), binding.clone(), venue, prefix) {
        Ok(_) | Err(Stage7GridError::Writer) => Err(error),
        Err(_) => Err(Stage7GridError::CanaryCleanup),
    }
}
