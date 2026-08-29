use std::{path::PathBuf, thread, time::Duration};

use crate::{
    domain::{FieldState, NativeOrderFamily},
    exchange::{
        binance_private::{AlgoOrderReadback, ConditionalStrategyStatus, parse_open_algo_orders},
        grid::{GridOrderFamilySnapshot, HedgedGridVenue},
    },
};

use super::*;

const EXTERNAL_ALGO_CLEANUP_FILE: &str = "external_algo_cleanup.jsonl";
const POST_CANCEL_READBACK_ATTEMPTS: usize = 10;
const POST_CANCEL_READBACK_DELAY_MS: u64 = 100;
const OBSERVATION_VALIDITY_MS: u64 = 30_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7ExternalAlgoCleanupRequest {
    pub artifacts_root: PathBuf,
    pub expected_client_algo_id: String,
    pub expected_algo_id: String,
    pub confirm_mainnet_external_algo_cancel: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Stage7ExternalAlgoCleanupReport {
    pub exchange: String,
    pub symbol: String,
    pub client_algo_id: String,
    pub algo_id: String,
    pub regular_orders_preserved: usize,
    pub private_generation: u64,
    pub already_absent: bool,
    pub cleanup_journal_path: PathBuf,
}

/// Cancels one operator-identified external Binance Algo only after the regular grid surface is
/// WAL-bound and the complete current Algo family proves that exact identity is its sole row.
/// The canonical writer lock spans pre-readback, prewrite, dispatch, and signed reconciliation.
pub fn run_binance_stage7_external_algo_cleanup(
    cfg: &Config,
    request: Stage7ExternalAlgoCleanupRequest,
) -> Result<Stage7ExternalAlgoCleanupReport, Stage7GridError> {
    if !request.confirm_mainnet_external_algo_cancel {
        return Err(Stage7GridError::ExternalAlgoConfirmation);
    }
    if !request.artifacts_root.is_absolute()
        || request.expected_client_algo_id.trim().is_empty()
        || request.expected_algo_id.trim().is_empty()
    {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let binding = binance_binding(cfg)?;
    let writer_scope = stage7_writer_scope(&binding);
    let _canonical_root = acquire_stage7_writer_root(&writer_scope, &request.artifacts_root)?;
    let recovery_scope = RecoveryWriterScope {
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
    };
    let authority = RecoveryWriterAuthority::open(
        request.artifacts_root.join(WRITER_FILE),
        recovery_scope.clone(),
    )?;
    let writer_guard = authority.lock_external_algo_cleanup()?;

    let checkpoint = ProjectionStore::new(request.artifacts_root.join(CHECKPOINT_FILE))
        .load::<Stage7GridCheckpoint>()?
        .filter(|checkpoint| {
            checkpoint.schema_version == 1
                && checkpoint.binding == binding
                && checkpoint.state.binding == binding
        })
        .ok_or(Stage7GridError::Checkpoint)?;
    let commands = CommandJournal::open(request.artifacts_root.join(COMMAND_FILE))?;
    if commands.has_unresolved() {
        return Err(Stage7GridError::Unresolved);
    }
    let cleanup_journal_path = request.artifacts_root.join(EXTERNAL_ALGO_CLEANUP_FILE);
    let mut cleanup = ExternalAlgoCleanupJournal::open(&cleanup_journal_path)?;
    cleanup.require_target(&request.expected_client_algo_id, &request.expected_algo_id)?;
    let mut evidence = open_stage7_private_evidence(&request.artifacts_root, &binding)?;
    let mut venue = BinanceGridVenue::production(binding.symbol.clone(), 1)?;

    let observed_at_ms = wall_clock_ms()?;
    let readback = venue.readback()?;
    let generation = record_readback(&mut evidence, &checkpoint, observed_at_ms, &readback)?;
    validate_regular_surface(&checkpoint, &commands, &readback, &binding)?;
    let (algos, signed_payload_sha256) = signed_algo_surface(&readback, &binding.symbol)?;
    if algos.is_empty() {
        if cleanup.latest().is_some() && !cleanup.is_settled() {
            cleanup.mark_settled_absent(signed_payload_sha256, observed_at_ms)?;
        }
        return Ok(report(
            &binding,
            &request,
            readback.orders.len(),
            generation,
            true,
            cleanup_journal_path,
        ));
    }
    let algo = exact_sole_target(&algos, &request)?;
    let custody = custody(&binding, algo)?;
    if cleanup.is_settled() {
        return Err(Stage7GridError::ExternalAlgoTarget);
    }
    if cleanup.latest().is_some()
        && !matches!(
            cleanup.latest().map(|record| &record.state),
            Some(ExternalAlgoCleanupState::StillOpen { .. })
        )
    {
        cleanup.mark_still_open(signed_payload_sha256.clone(), observed_at_ms)?;
    }
    let signature = venue.sign_recovery_payload_sha256(&signed_payload_sha256)?;
    let signature_verified =
        venue.verify_recovery_payload_signature(&signed_payload_sha256, &signature);
    let proof = RecoveryObservationProof {
        generation,
        observed_at_ms,
        valid_until_ms: observed_at_ms
            .checked_add(OBSERVATION_VALIDITY_MS)
            .ok_or(Stage7GridError::Clock)?,
        payload_sha256: signed_payload_sha256,
        signature_verified,
    };
    let dispatch_at_ms = wall_clock_ms()?;
    let authorization = authorize_external_algo_cancel(ExternalAlgoCancelInput {
        scope: &recovery_scope,
        custody: &custody,
        proof: &proof,
        now_ms: dispatch_at_ms,
    })?;
    let client = venue.mutation_client();
    let dispatch_result = submit_external_algo_cancel(
        &mut cleanup,
        client.as_ref(),
        authorization,
        dispatch_at_ms,
        &writer_guard,
    );

    for attempt in 0..POST_CANCEL_READBACK_ATTEMPTS {
        if attempt > 0 {
            thread::sleep(Duration::from_millis(POST_CANCEL_READBACK_DELAY_MS));
        }
        let post_observed_at_ms = wall_clock_ms()?;
        let post = venue.readback()?;
        let post_generation =
            record_readback(&mut evidence, &checkpoint, post_observed_at_ms, &post)?;
        validate_regular_surface(&checkpoint, &commands, &post, &binding)?;
        let (post_algos, post_payload_sha256) = signed_algo_surface(&post, &binding.symbol)?;
        if post_algos.is_empty() {
            cleanup.mark_settled_absent(post_payload_sha256, post_observed_at_ms)?;
            return Ok(report(
                &binding,
                &request,
                post.orders.len(),
                post_generation,
                false,
                cleanup_journal_path,
            ));
        }
        let _ = exact_sole_target(&post_algos, &request)?;
    }
    if let Err(error) = dispatch_result {
        return Err(Stage7GridError::ExternalAlgoCleanup(error));
    }
    Err(Stage7GridError::ExternalAlgoUnresolved)
}

fn validate_regular_surface(
    checkpoint: &Stage7GridCheckpoint,
    commands: &CommandJournal,
    readback: &GridVenueReadback,
    binding: &HedgedGridBinding,
) -> Result<(), Stage7GridError> {
    require_complete_order_family_readback(readback)?;
    verify_readback_scope(&checkpoint.state, commands, readback, binding)
}

fn signed_algo_surface(
    readback: &GridVenueReadback,
    symbol: &crate::domain::Symbol,
) -> Result<(Vec<AlgoOrderReadback>, String), Stage7GridError> {
    let family = readback
        .order_family_readback
        .as_ref()
        .and_then(|families| families.snapshot(NativeOrderFamily::UmAlgo))
        .ok_or(Stage7GridError::OrderFamily)?;
    let GridOrderFamilySnapshot::Complete {
        orders,
        signed_payloads,
    } = family
    else {
        return Err(Stage7GridError::OrderFamily);
    };
    if signed_payloads.len() != 1 {
        return Err(Stage7GridError::OrderFamily);
    }
    let algos = parse_open_algo_orders(&signed_payloads[0], symbol)
        .map_err(|_| Stage7GridError::OrderFamily)?;
    if algos.len() != orders.len() {
        return Err(Stage7GridError::OrderFamily);
    }
    Ok((algos, sha256_hex(signed_payloads[0].as_bytes())))
}

fn exact_sole_target<'a>(
    algos: &'a [AlgoOrderReadback],
    request: &Stage7ExternalAlgoCleanupRequest,
) -> Result<&'a AlgoOrderReadback, Stage7GridError> {
    match algos {
        [algo]
            if algo.status == ConditionalStrategyStatus::Current
                && algo.client_algo_id == request.expected_client_algo_id
                && algo.algo_id == request.expected_algo_id =>
        {
            Ok(algo)
        }
        _ => Err(Stage7GridError::ExternalAlgoTarget),
    }
}

fn custody(
    binding: &HedgedGridBinding,
    algo: &AlgoOrderReadback,
) -> Result<ExternalAlgoCustody, Stage7GridError> {
    let (
        FieldState::Known(order_type),
        FieldState::Known(side),
        FieldState::Known(position_side),
        FieldState::Known(quantity),
        FieldState::Known(trigger_price),
        FieldState::Known(working_type),
        FieldState::Known(close_position),
        FieldState::Known(reduce_only),
    ) = (
        &algo.order_type,
        &algo.side,
        &algo.position_side,
        &algo.quantity,
        &algo.trigger_price,
        &algo.working_type,
        &algo.close_position,
        &algo.reduce_only,
    )
    else {
        return Err(Stage7GridError::ExternalAlgoTarget);
    };
    Ok(ExternalAlgoCustody {
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        algo_id: algo.algo_id.clone(),
        client_algo_id: algo.client_algo_id.clone(),
        order_type: order_type.clone(),
        side: *side,
        position_side: *position_side,
        quantity: *quantity,
        trigger_price: *trigger_price,
        working_type: working_type.clone(),
        close_position: *close_position,
        reduce_only: *reduce_only,
    })
}

fn report(
    binding: &HedgedGridBinding,
    request: &Stage7ExternalAlgoCleanupRequest,
    regular_orders_preserved: usize,
    private_generation: u64,
    already_absent: bool,
    cleanup_journal_path: PathBuf,
) -> Stage7ExternalAlgoCleanupReport {
    Stage7ExternalAlgoCleanupReport {
        exchange: binding.exchange.clone(),
        symbol: binding.symbol.to_string(),
        client_algo_id: request.expected_client_algo_id.clone(),
        algo_id: request.expected_algo_id.clone(),
        regular_orders_preserved,
        private_generation,
        already_absent,
        cleanup_journal_path,
    }
}
