use std::path::Path;

use tracing::{info, warn};

use crate::runtime::hedged_grid;

use super::*;

pub(super) const RISK_RECEIPT_FILE: &str = "exposure_take_profit.jsonl";

pub(super) fn initialize_exposure_guard(
    cfg: &Config,
    binding: &HedgedGridBinding,
    checkpoint: &mut Stage7GridCheckpoint,
    artifacts_root: &Path,
) -> Result<Option<hedged_grid::ExposureRuntimeSettings>, Stage7GridError> {
    let Some(config) = cfg.hedged_grid.and_then(|grid| grid.exposure_take_profit) else {
        if checkpoint.exposure_guard.is_some() || checkpoint.pending_exposure_reduction.is_some() {
            return Err(Stage7GridError::Checkpoint);
        }
        return Ok(None);
    };
    let settings = hedged_grid::ExposureRuntimeSettings::try_from(config)?;
    match checkpoint.exposure_guard.as_ref() {
        None => {
            checkpoint.exposure_guard =
                Some(crate::strategy::hedged_grid::ExposureGuardState::new(
                    binding.clone(),
                    settings.guard.clone(),
                )?);
        }
        Some(guard) if guard.binding == *binding && guard.params == settings.guard => {
            guard.validate_checkpoint()?;
        }
        Some(guard) if guard.binding == *binding => {
            guard.validate_checkpoint()?;
            let stopped_and_drained = checkpoint.state.phase == GridPhase::Stopping
                && checkpoint.state.owned_orders.is_empty()
                && checkpoint.state.pending_transactions.is_empty()
                && checkpoint.state.pending_replenishments.is_empty();
            if stopped_and_drained {
                retire_settled_pending_for_release_migration(checkpoint, artifacts_root)?;
            }
            let release_migration_is_safe =
                stopped_and_drained && checkpoint.pending_exposure_reduction.is_none();
            if !release_migration_is_safe {
                return Err(Stage7GridError::Checkpoint);
            }
            checkpoint
                .exposure_guard
                .as_mut()
                .ok_or(Stage7GridError::Checkpoint)?
                .migrate_release_params(settings.guard.clone())?;
        }
        Some(_) => return Err(Stage7GridError::Checkpoint),
    }
    validate_exposure_checkpoint(checkpoint, binding)?;
    Ok(Some(settings))
}

fn retire_settled_pending_for_release_migration(
    checkpoint: &mut Stage7GridCheckpoint,
    artifacts_root: &Path,
) -> Result<(), Stage7GridError> {
    let Some(pending) = checkpoint.pending_exposure_reduction.as_ref() else {
        return Ok(());
    };
    pending
        .validate_identity()
        .map_err(|_| Stage7GridError::Checkpoint)?;
    let command = pending
        .command
        .as_ref()
        .ok_or(Stage7GridError::Checkpoint)?;
    let lane = match pending.action.position {
        GridPosition::Long => {
            &checkpoint
                .exposure_guard
                .as_ref()
                .ok_or(Stage7GridError::Checkpoint)?
                .long
                .state
        }
        GridPosition::Short => {
            &checkpoint
                .exposure_guard
                .as_ref()
                .ok_or(Stage7GridError::Checkpoint)?
                .short
                .state
        }
    };
    if !matches!(
        lane,
        crate::strategy::hedged_grid::ExposureEpisodeState::Latched { risk_episode_id }
            if risk_episode_id == &pending.action.risk_episode_id
    ) {
        return Err(Stage7GridError::Checkpoint);
    }
    let audit = hedged_grid::reduction_audit_for_episode(
        &artifacts_root.join(RISK_RECEIPT_FILE),
        &pending.action.risk_episode_id,
    )?
    .ok_or(Stage7GridError::Checkpoint)?;
    if audit.exchange != pending.review_account.exchange
        || audit.account != pending.review_account.account
        || audit.symbol != pending.review_leg.symbol
        || audit.position_side != pending.review_leg.position_side
        || audit.trigger_generation != pending.action.trigger_generation
        || audit.requested_reduce_ratio != pending.action.reduce_ratio
        || audit.executed_reduce_quantity <= rust_decimal::Decimal::ZERO
        || audit.executed_reduce_quantity > command.quantity
    {
        return Err(Stage7GridError::Checkpoint);
    }
    checkpoint.pending_exposure_reduction = None;
    Ok(())
}

pub(super) fn validate_exposure_checkpoint(
    checkpoint: &Stage7GridCheckpoint,
    binding: &HedgedGridBinding,
) -> Result<(), Stage7GridError> {
    let guard = checkpoint
        .exposure_guard
        .as_ref()
        .ok_or(Stage7GridError::Checkpoint)?;
    guard
        .validate_checkpoint()
        .map_err(|_| Stage7GridError::Checkpoint)?;
    if guard.binding != *binding {
        return Err(Stage7GridError::Checkpoint);
    }
    let is_inflight = |state: &crate::strategy::hedged_grid::ExposureEpisodeState| {
        matches!(
            state,
            crate::strategy::hedged_grid::ExposureEpisodeState::TriggerPersisted { .. }
                | crate::strategy::hedged_grid::ExposureEpisodeState::Reducing { .. }
                | crate::strategy::hedged_grid::ExposureEpisodeState::Reconciling { .. }
        )
    };
    let Some(pending) = &checkpoint.pending_exposure_reduction else {
        if is_inflight(&guard.long.state) || is_inflight(&guard.short.state) {
            return Err(Stage7GridError::Checkpoint);
        }
        return Ok(());
    };
    pending
        .validate_identity()
        .map_err(|_| Stage7GridError::Checkpoint)?;
    if pending.review_account.exchange != binding.exchange
        || pending.review_account.account != binding.account
        || pending.review_leg.symbol != binding.symbol
    {
        return Err(Stage7GridError::Checkpoint);
    }
    if let Some(command) = &pending.command
        && (command.owner.strategy_instance_id != binding.strategy_instance_id
            || command.owner.run_id != binding.run_id
            || command.owner.exchange != binding.exchange
            || command.owner.account != binding.account
            || command.owner.symbol != binding.symbol)
    {
        return Err(Stage7GridError::Checkpoint);
    }
    let (lane, other) = match pending.action.position {
        GridPosition::Long => (&guard.long.state, &guard.short.state),
        GridPosition::Short => (&guard.short.state, &guard.long.state),
    };
    if is_inflight(other) {
        return Err(Stage7GridError::Checkpoint);
    }
    let episode_matches = |risk_episode_id: &str| risk_episode_id == pending.action.risk_episode_id;
    let valid_state = match (&pending.command, lane) {
        (
            None,
            crate::strategy::hedged_grid::ExposureEpisodeState::TriggerPersisted {
                risk_episode_id,
            },
        ) => episode_matches(risk_episode_id),
        (
            Some(_),
            crate::strategy::hedged_grid::ExposureEpisodeState::Reducing { risk_episode_id }
            | crate::strategy::hedged_grid::ExposureEpisodeState::Reconciling { risk_episode_id }
            | crate::strategy::hedged_grid::ExposureEpisodeState::Latched { risk_episode_id },
        ) => episode_matches(risk_episode_id),
        _ => false,
    };
    if !valid_state {
        return Err(Stage7GridError::Checkpoint);
    }
    Ok(())
}

pub(super) fn recover_unjournaled_exposure(
    checkpoint: &mut Stage7GridCheckpoint,
    checkpoint_store: &ProjectionStore,
    commands: &CommandJournal,
) -> Result<(), Stage7GridError> {
    let Some(pending) = checkpoint.pending_exposure_reduction.as_mut() else {
        return Ok(());
    };
    let Some(command) = pending.command.as_ref() else {
        return Ok(());
    };
    if commands.receipt(&command.command_id).is_some() {
        return Ok(());
    }
    checkpoint
        .exposure_guard
        .as_mut()
        .ok_or(Stage7GridError::Checkpoint)?
        .recover_unprepared_trigger(pending.action.position, &pending.action.risk_episode_id)?;
    pending.command = None;
    save_checkpoint(checkpoint_store, checkpoint)
}

fn record_risk_readback<V: HedgedGridVenue>(
    checkpoint: &mut Stage7GridCheckpoint,
    evidence: &mut PrivateEvidenceJournal,
    venue: &mut V,
    binding: &HedgedGridBinding,
    now_ms: u64,
) -> Result<
    (
        crate::exchange::grid::GridRiskReadback,
        Vec<hedged_grid::RawRiskEvidenceRef>,
    ),
    Stage7GridError,
> {
    let generation = checkpoint
        .private_generation
        .max(evidence.last_generation())
        .checked_add(1)
        .ok_or(Stage7GridError::Clock)?;
    let readback = venue.risk_readback(&binding.account, generation)?;
    persist_risk_readback(checkpoint, evidence, readback, generation, now_ms)
}

fn persist_risk_readback(
    checkpoint: &mut Stage7GridCheckpoint,
    evidence: &mut PrivateEvidenceJournal,
    readback: crate::exchange::grid::GridRiskReadback,
    generation: u64,
    now_ms: u64,
) -> Result<
    (
        crate::exchange::grid::GridRiskReadback,
        Vec<hedged_grid::RawRiskEvidenceRef>,
    ),
    Stage7GridError,
> {
    if readback.raw_private_payloads.is_empty()
        || readback.account.private_generation != generation
        || readback
            .legs
            .iter()
            .any(|leg| leg.private_generation != generation)
    {
        return Err(Stage7GridError::PrivateEvidence);
    }
    let mut raw_evidence = Vec::with_capacity(readback.raw_private_payloads.len());
    for payload in &readback.raw_private_payloads {
        let sequence =
            evidence.append(PrivateEvidence::new(generation, now_ms, payload.clone())?)?;
        raw_evidence.push(hedged_grid::RawRiskEvidenceRef {
            sequence,
            generation,
            payload_sha256: sha256_hex(payload.as_bytes()),
        });
    }
    checkpoint.private_generation = generation;
    Ok((readback, raw_evidence))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn poll_exposure_take_profit<V: HedgedGridVenue>(
    settings: &hedged_grid::ExposureRuntimeSettings,
    checkpoint: &mut Stage7GridCheckpoint,
    checkpoint_store: &ProjectionStore,
    commands: &mut CommandJournal,
    evidence: &mut PrivateEvidenceJournal,
    shadow_evidence: &mut hedged_grid::ExposureShadowEvidenceJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &mut Option<WriterSession>,
    binding: &HedgedGridBinding,
    shadow_only: bool,
    now_ms: u64,
    prefetched_readback: Option<crate::exchange::grid::GridRiskReadback>,
) -> Result<bool, Stage7GridError> {
    if !(shadow_only || settings.shadow)
        && checkpoint
            .exposure_guard
            .as_mut()
            .ok_or(Stage7GridError::Checkpoint)?
            .release_shadow_latches()
    {
        save_checkpoint(checkpoint_store, checkpoint)?;
    }
    let resumed = checkpoint.pending_exposure_reduction.clone();
    if resumed
        .as_ref()
        .is_some_and(|pending| pending.command.is_some())
    {
        return Ok(true);
    }
    let (action, trigger_account) = if let Some(pending) = resumed {
        (pending.action, pending.review_account)
    } else {
        let (readback, raw_evidence) = match prefetched_readback {
            Some(readback) => {
                let generation = checkpoint
                    .private_generation
                    .max(evidence.last_generation())
                    .checked_add(1)
                    .ok_or(Stage7GridError::Clock)?;
                persist_risk_readback(checkpoint, evidence, readback, generation, now_ms)?
            }
            None => record_risk_readback(checkpoint, evidence, venue, binding, now_ms)?,
        };
        // Risk readback can span several signed requests. Validate against a post-read host clock;
        // using the turn-start clock falsely classifies normal request latency as future evidence.
        let snapshot_now_ms = wall_clock_ms()?;
        let selected = hedged_grid::select_binding_risk_snapshot(
            &readback,
            binding,
            snapshot_now_ms,
            settings.guard.max_snapshot_age_ms,
        )?;
        let mut trigger = None;
        let mut evaluations = Vec::new();
        for position in [GridPosition::Long, GridPosition::Short] {
            let decision = if let Some(leg) = selected.leg(position) {
                checkpoint
                    .exposure_guard
                    .as_mut()
                    .ok_or(Stage7GridError::Checkpoint)?
                    .evaluate(&selected.account, leg, selected.validated_at_ms)?
            } else {
                checkpoint
                    .exposure_guard
                    .as_mut()
                    .ok_or(Stage7GridError::Checkpoint)?
                    .observe_flat(position, selected.account.private_generation)?;
                crate::strategy::hedged_grid::ExposureGuardDecision::Noop
            };
            if trigger.is_none()
                && let crate::strategy::hedged_grid::ExposureGuardDecision::ReduceProfitableExposure(
                    action,
                ) = &decision
                && let Some(leg) = selected.leg(position)
            {
                trigger = Some((action.clone(), leg.clone()));
            }
            evaluations.push((position, decision));
        }
        if shadow_only || settings.shadow {
            for (position, decision) in &evaluations {
                shadow_evidence.append_if_changed(hedged_grid::build_shadow_evidence(
                    binding,
                    &settings.guard,
                    &selected.account,
                    *position,
                    selected.leg(*position),
                    decision,
                    selected.validated_at_ms,
                    raw_evidence.clone(),
                )?)?;
            }
        }
        let Some((action, trigger_leg)) = trigger else {
            save_checkpoint(checkpoint_store, checkpoint)?;
            return Ok(false);
        };
        checkpoint.pending_exposure_reduction = Some(hedged_grid::ExposureReductionPending {
            action: action.clone(),
            review_account: selected.account.clone(),
            review_leg: trigger_leg,
            command: None,
        });
        save_checkpoint(checkpoint_store, checkpoint)?;
        (action, selected.account)
    };

    if shadow_only || settings.shadow {
        info!(
            event = "grid_exposure_take_profit_would_trigger",
            exchange = %binding.exchange,
            account = %binding.account,
            symbol = %binding.symbol,
            position_side = ?action.position,
            account_equity = %trigger_account.account_equity,
            position_unrealized_pnl = %action.unrealized_pnl,
            position_notional = %action.position_notional,
            risk_episode_id = %action.risk_episode_id,
            "高暴露浮盈减仓影子命中"
        );
        checkpoint
            .exposure_guard
            .as_mut()
            .ok_or(Stage7GridError::Checkpoint)?
            .mark_shadow_latched(action.position, &action.risk_episode_id)?;
        checkpoint.pending_exposure_reduction = None;
        save_checkpoint(checkpoint_store, checkpoint)?;
        return Ok(false);
    }

    venue.verify_current_instrument_rules()?;
    let order_review_now_ms = wall_clock_ms()?;
    let order_review = venue.readback()?;
    require_complete_order_family_readback(&order_review)?;
    verify_readback_scope(&checkpoint.state, commands, &order_review, binding)?;
    checkpoint.private_generation =
        record_readback(evidence, checkpoint, order_review_now_ms, &order_review)?;
    save_checkpoint(checkpoint_store, checkpoint)?;
    let review_now_ms = wall_clock_ms()?;
    let (review, _) = record_risk_readback(checkpoint, evidence, venue, binding, review_now_ms)?;
    let review_validation_now_ms = wall_clock_ms()?;
    let selected = hedged_grid::select_binding_risk_snapshot(
        &review,
        binding,
        review_validation_now_ms,
        settings.guard.max_snapshot_age_ms,
    )?;
    let Some(review_leg) = selected.leg(action.position).cloned() else {
        return cancel_before_submit(
            checkpoint,
            checkpoint_store,
            action.position,
            &action.risk_episode_id,
            selected.account.private_generation,
        );
    };
    let mut proof = crate::strategy::hedged_grid::ExposureGuardState::new(
        binding.clone(),
        settings.guard.clone(),
    )?;
    if !matches!(
        proof.evaluate(
            &selected.account,
            &review_leg,
            selected.validated_at_ms,
        )?,
        crate::strategy::hedged_grid::ExposureGuardDecision::ReduceProfitableExposure(ref fresh)
            if fresh.position == action.position
    ) {
        return cancel_before_submit(
            checkpoint,
            checkpoint_store,
            action.position,
            &action.risk_episode_id,
            selected.account.private_generation,
        );
    }
    let plan = hedged_grid::plan_market_reduction(
        binding,
        &action,
        &selected.account,
        &review_leg,
        venue.instrument(),
        selected.validated_at_ms,
        settings.guard.max_snapshot_age_ms,
    )?;
    let hedged_grid::MarketReductionPlan::Authorized { command, .. } = plan else {
        warn!(
            event = "grid_exposure_take_profit_skipped_below_minimum",
            exchange = %binding.exchange,
            symbol = %binding.symbol,
            risk_episode_id = %action.risk_episode_id,
            "规整后的风险减仓量低于可关闭最小量"
        );
        return cancel_before_submit(
            checkpoint,
            checkpoint_store,
            action.position,
            &action.risk_episode_id,
            selected.account.private_generation,
        );
    };
    checkpoint.pending_exposure_reduction = Some(hedged_grid::ExposureReductionPending {
        action: action.clone(),
        review_account: selected.account,
        review_leg,
        command: Some(command.clone()),
    });
    checkpoint
        .exposure_guard
        .as_mut()
        .ok_or(Stage7GridError::Checkpoint)?
        .mark_reducing(action.position, &action.risk_episode_id)?;
    save_checkpoint(checkpoint_store, checkpoint)?;
    *writer = Some(active_writer(
        authority,
        writer.take(),
        wall_clock_ms()?,
        checkpoint.private_generation,
    )?);
    execute_mutations(
        commands,
        venue,
        authority,
        writer.as_ref().ok_or(Stage7GridError::Writer)?,
        vec![Stage7Mutation::Reduce(command)],
        true,
    )?;
    checkpoint
        .exposure_guard
        .as_mut()
        .ok_or(Stage7GridError::Checkpoint)?
        .mark_reconciling(action.position, &action.risk_episode_id)?;
    save_checkpoint(checkpoint_store, checkpoint)?;
    Ok(true)
}

fn cancel_before_submit(
    checkpoint: &mut Stage7GridCheckpoint,
    checkpoint_store: &ProjectionStore,
    position: GridPosition,
    risk_episode_id: &str,
    generation: u64,
) -> Result<bool, Stage7GridError> {
    checkpoint
        .exposure_guard
        .as_mut()
        .ok_or(Stage7GridError::Checkpoint)?
        .mark_latched(position, risk_episode_id, generation)?;
    checkpoint.pending_exposure_reduction = None;
    save_checkpoint(checkpoint_store, checkpoint)?;
    Ok(false)
}

#[cfg(test)]
pub(super) fn settle_exposure_take_profit<V: HedgedGridVenue>(
    checkpoint: &mut Stage7GridCheckpoint,
    checkpoint_store: &ProjectionStore,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
    artifacts_root: &Path,
    settled_generation: u64,
) -> Result<bool, Stage7GridError> {
    Ok(matches!(
        settle_exposure_take_profit_with_public_refresh(
            checkpoint,
            checkpoint_store,
            commands,
            venue,
            authority,
            writer,
            binding,
            readback,
            artifacts_root,
            settled_generation,
            |_| Ok(true),
        )?,
        ExposureSettlement::PrivateReadbackRequired
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ExposureSettlement {
    Complete,
    PrivateReadbackRequired,
    PublicDeferred,
}

pub(super) fn latched_exposure_repair_pending(
    checkpoint: &Stage7GridCheckpoint,
) -> Result<bool, Stage7GridError> {
    let Some(pending) = checkpoint.pending_exposure_reduction.as_ref() else {
        return Ok(false);
    };
    if checkpoint.state.phase != GridPhase::Running {
        return Ok(false);
    }
    let guard = checkpoint
        .exposure_guard
        .as_ref()
        .ok_or(Stage7GridError::Checkpoint)?;
    let state = match pending.action.position {
        GridPosition::Long => &guard.long.state,
        GridPosition::Short => &guard.short.state,
    };
    Ok(matches!(
        state,
        crate::strategy::hedged_grid::ExposureEpisodeState::Latched { .. }
    ))
}

#[allow(clippy::too_many_arguments)]
pub(super) fn settle_exposure_take_profit_with_public_refresh<V, F>(
    checkpoint: &mut Stage7GridCheckpoint,
    checkpoint_store: &ProjectionStore,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
    artifacts_root: &Path,
    settled_generation: u64,
    refresh_public: F,
) -> Result<ExposureSettlement, Stage7GridError>
where
    V: HedgedGridVenue,
    F: FnOnce(&mut V) -> Result<bool, Stage7GridError>,
{
    let Some(pending) = checkpoint.pending_exposure_reduction.clone() else {
        return Ok(ExposureSettlement::Complete);
    };
    let Some(command) = pending.command.as_ref() else {
        return Ok(ExposureSettlement::PrivateReadbackRequired);
    };
    let receipt_state = commands
        .receipt(&command.command_id)
        .map(|receipt| receipt.state.clone());
    let episode_state = match pending.action.position {
        GridPosition::Long => checkpoint
            .exposure_guard
            .as_ref()
            .ok_or(Stage7GridError::Checkpoint)?
            .long
            .state
            .clone(),
        GridPosition::Short => checkpoint
            .exposure_guard
            .as_ref()
            .ok_or(Stage7GridError::Checkpoint)?
            .short
            .state
            .clone(),
    };
    if matches!(
        episode_state,
        crate::strategy::hedged_grid::ExposureEpisodeState::Latched { .. }
    ) {
        if signed_complete_owned_fill_present(&checkpoint.state.owned_orders, &readback.fills) {
            // A maker execution may arrive in the first signed generation after the risk
            // episode latched. Yield this turn to the resident fill driver; repairing the
            // physical ladder first would erase the ordering boundary promised to that fill.
            return Ok(ExposureSettlement::Complete);
        }
        return repair_latched_exposure(
            checkpoint,
            checkpoint_store,
            commands,
            venue,
            authority,
            writer,
            binding,
            readback,
            settled_generation,
            refresh_public,
        );
    }
    if matches!(
        episode_state,
        crate::strategy::hedged_grid::ExposureEpisodeState::Reducing { .. }
    ) && matches!(
        receipt_state,
        Some(CommandState::Accepted { .. } | CommandState::Rejected { .. })
    ) {
        checkpoint
            .exposure_guard
            .as_mut()
            .ok_or(Stage7GridError::Checkpoint)?
            .mark_reconciling(pending.action.position, &pending.action.risk_episode_id)?;
        save_checkpoint(checkpoint_store, checkpoint)?;
    }
    let fills = resolve_grid_fill_client_ids(commands, &readback.fills)
        .iter()
        .filter(|record| {
            matches!(
                &record.client_order_id,
                FieldState::Known(client_id) if client_id == command.client_order_id.as_str()
            )
        })
        .map(|record| hedged_grid::associate_reduction_fill(command, record.fill.clone()))
        .collect::<Vec<_>>();
    let terminal = if matches!(receipt_state, Some(CommandState::Rejected { .. })) {
        true
    } else {
        match venue.order_by_client_id(command.client_order_id.as_str()) {
            Ok(order) => matches!(
                order.state,
                OrderState::Filled
                    | OrderState::Cancelled
                    | OrderState::Expired
                    | OrderState::Rejected
            ),
            Err(error) if is_order_absent(&error) => !fills.is_empty(),
            Err(_) => false,
        }
    };
    if !terminal {
        return Ok(ExposureSettlement::PrivateReadbackRequired);
    }
    if fills.is_empty() {
        warn!(
            event = "grid_exposure_take_profit_zero_fill",
            exchange = %pending.review_account.exchange,
            symbol = %pending.review_leg.symbol,
            risk_episode_id = %pending.action.risk_episode_id,
            "风险减仓终态无成交，不生成成功收据"
        );
    } else {
        let audit = hedged_grid::summarize_reduction_fills(
            command,
            &pending.action,
            &pending.review_account,
            &pending.review_leg,
            &fills,
            settled_generation,
        )?;
        let receipt_written = append_audit(artifacts_root, &audit)?;
        if receipt_written {
            info!(
            event = %audit.event,
            exchange = %audit.exchange,
            account = %audit.account,
            symbol = %audit.symbol,
            position_side = ?audit.position_side,
            account_equity = %audit.account_equity,
            position_unrealized_pnl = %audit.position_unrealized_pnl,
            position_notional_before = %audit.position_notional_before,
            requested_reduce_ratio = %audit.requested_reduce_ratio,
            executed_reduce_quantity = %audit.executed_reduce_quantity,
            executed_reduce_notional = %audit.executed_reduce_notional,
            average_fill_price = %audit.average_fill_price.value(),
            risk_currency = %audit.risk_currency,
            risk_episode_id = %audit.risk_episode_id,
            trigger_generation = audit.trigger_generation,
            settled_generation = audit.settled_generation,
                "高暴露浮盈仓位已按实际成交降险"
            );
        }
    }
    checkpoint
        .exposure_guard
        .as_mut()
        .ok_or(Stage7GridError::Checkpoint)?
        .mark_latched(
            pending.action.position,
            &pending.action.risk_episode_id,
            settled_generation,
        )?;
    save_checkpoint(checkpoint_store, checkpoint)?;
    // Keep the episode identity through one ordinary resident turn. That turn replays every
    // strictly ordered maker fill first; the next signed generation performs the same-anchor
    // physical ladder repair. The risk taker itself never enters the grid fill parser.
    Ok(ExposureSettlement::Complete)
}

#[allow(clippy::too_many_arguments)]
fn repair_latched_exposure<V, F>(
    checkpoint: &mut Stage7GridCheckpoint,
    checkpoint_store: &ProjectionStore,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
    settled_generation: u64,
    refresh_public: F,
) -> Result<ExposureSettlement, Stage7GridError>
where
    V: HedgedGridVenue,
    F: FnOnce(&mut V) -> Result<bool, Stage7GridError>,
{
    if checkpoint.state.phase == GridPhase::ResettingGrid
        && checkpoint.state.owned_orders.is_empty()
        && checkpoint.state.pending_transactions.is_empty()
        && checkpoint.state.pending_replenishments.is_empty()
    {
        // A reset with no physical ladder has nothing to repair. Clearing only the runtime
        // pending envelope leaves the guard episode latched and preserves the recovery anchor;
        // the risk taker therefore cannot become a grid fill or trigger an epoch transition.
        checkpoint.pending_exposure_reduction = None;
        save_checkpoint(checkpoint_store, checkpoint)?;
        return Ok(ExposureSettlement::Complete);
    }
    if checkpoint.state.phase != GridPhase::Running
        || !checkpoint.state.pending_transactions.is_empty()
    {
        return Ok(ExposureSettlement::Complete);
    }
    let signed = recovered_owned_orders(commands, binding, readback)?;
    // Signed REST, WAL recovery and terminal-order lookup can outlive the public freshness
    // window. Persist the frames received during that slow work before the physical repair
    // samples its BBO. This refresh grants no mutation authority of its own.
    let public_ready = refresh_public(venue)?;
    let now_ms = wall_clock_ms()?;
    let book = if public_ready {
        venue.best_bid_ask(now_ms)
    } else {
        Err(GridVenueError::PublicNotReady)
    };
    let (bid, ask) = match book {
        Ok(book) => book,
        Err(GridVenueError::PublicNotReady) => {
            // The reduce-only command is already terminal and durably latched. A temporary
            // public-book gap cannot invalidate that private fact, and it must not terminate the
            // resident. Keep the repair envelope intact and retry from a newer signed generation;
            // no ladder mutation is prepared without a fresh BBO.
            warn!(
                event = "stage7_exposure_repair_public_backoff",
                exchange = venue.exchange(),
                "risk reduction is settled but the same-anchor ladder repair is waiting for a fresh public book"
            );
            return Ok(ExposureSettlement::PublicDeferred);
        }
        Err(error) => return Err(error.into()),
    };
    let final_inventory = inventory(
        readback,
        settled_generation,
        now_ms,
        bid,
        ask,
        &binding.symbol,
    )?;
    let hedged_grid::ExposureLadderRepairPlan::Ready {
        target,
        cancel,
        place,
    } = hedged_grid::plan_same_anchor_exposure_repair(
        &checkpoint.state,
        &final_inventory,
        &signed,
    )?
    else {
        return Ok(ExposureSettlement::Complete);
    };

    // A missing identity which already crossed WAL admission is an execution debt, not authority
    // to submit another order. Let the ordinary exact-order/fill reconciliation settle it.
    for intent in &place {
        let client_id = client_order_id(&intent.key)?;
        if commands.command_id_by_client_id(&client_id).is_some() {
            return Ok(ExposureSettlement::Complete);
        }
    }

    checkpoint.state.owned_orders = target;
    checkpoint.state.reconcile_order_sequences();
    save_checkpoint(checkpoint_store, checkpoint)?;

    let mut accepted_cancel_pending = false;
    let cancellations = cancel
        .iter()
        .filter_map(|key| {
            match crate::runtime::hedged_grid_live::accepted_cancel_exists(commands, binding, key) {
                Ok(true) => {
                    accepted_cancel_pending = true;
                    None
                }
                Ok(false) => Some(
                    next_cancel_command(commands, binding, key)
                        .map(Stage7Mutation::from_grid)
                        .map_err(Stage7GridError::Legacy),
                ),
                Err(error) => Some(Err(Stage7GridError::Legacy(error))),
            }
        })
        .collect::<Result<Vec<_>, Stage7GridError>>()?;
    let placements = place
        .iter()
        .map(|intent| {
            place_command(binding, venue.instrument(), intent)
                .map(Stage7Mutation::from_grid)
                .map_err(Stage7GridError::from)
        })
        .collect::<Result<Vec<_>, Stage7GridError>>()?;
    let dispatched = !cancellations.is_empty() || !placements.is_empty();
    if accepted_cancel_pending && !dispatched {
        return Ok(ExposureSettlement::PrivateReadbackRequired);
    }
    for mutations in [cancellations, placements] {
        if mutations.is_empty() {
            continue;
        }
        match execute_mutations(commands, venue, authority, writer, mutations, true) {
            Ok(()) => {}
            Err(Stage7GridError::Rejected | Stage7GridError::Unresolved) => {
                if checkpoint.state.phase == GridPhase::Running {
                    checkpoint.state.block_for_order_reconciliation()?;
                }
                save_checkpoint(checkpoint_store, checkpoint)?;
                return Ok(ExposureSettlement::PrivateReadbackRequired);
            }
            Err(error) => return Err(error),
        }
    }
    if dispatched {
        return Ok(ExposureSettlement::PrivateReadbackRequired);
    }

    checkpoint.pending_exposure_reduction = None;
    save_checkpoint(checkpoint_store, checkpoint)?;
    Ok(ExposureSettlement::Complete)
}

fn append_audit(
    artifacts_root: &Path,
    audit: &hedged_grid::ExposureReductionAudit,
) -> Result<bool, Stage7GridError> {
    let path = artifacts_root.join(RISK_RECEIPT_FILE);
    hedged_grid::append_reduction_audit_once(&path, audit).map_err(Into::into)
}
