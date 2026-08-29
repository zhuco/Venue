use super::*;

struct EpochInstallPlan {
    state: HedgedGridState,
    closing: Vec<Stage7Mutation>,
    opening: Vec<Stage7Mutation>,
    crossing: Option<(OrderSide, Price)>,
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(super) fn install_epoch<V: HedgedGridVenue>(
    checkpoint: &mut Stage7GridCheckpoint,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    inventory: &GridInventory,
    bid: Price,
    ask: Price,
    store: &ProjectionStore,
) -> Result<(), Stage7GridError> {
    install_epoch_with_public_refresh(
        checkpoint,
        commands,
        venue,
        authority,
        writer,
        binding,
        inventory,
        bid,
        ask,
        store,
        |_| Ok(true),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn install_epoch_with_public_refresh<V, F>(
    checkpoint: &mut Stage7GridCheckpoint,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    inventory: &GridInventory,
    bid: Price,
    ask: Price,
    store: &ProjectionStore,
    refresh_public: F,
) -> Result<(), Stage7GridError>
where
    V: HedgedGridVenue,
    F: FnOnce(&mut V) -> Result<bool, Stage7GridError>,
{
    let minimum_epoch = next_unused_grid_epoch(commands, binding)?;
    let exact_epoch = stage7_epoch(&checkpoint.state, venue, bid, ask, minimum_epoch)?;
    let mut plan = epoch_install_plan(checkpoint, venue, binding, exact_epoch, bid, ask)?;

    if let Some((crossing_side, crossing_limit_price)) = plan.crossing {
        let crate::strategy::hedged_grid::InventoryRecoveryState::Rebuilding {
            fill_id,
            fill_price,
        } = &checkpoint.state.inventory_recovery
        else {
            log_passive_wait(crossing_side, crossing_limit_price, bid, ask);
            return Ok(());
        };
        let mut fallback_epoch =
            stage7_midpoint_epoch(&checkpoint.state, venue, bid, ask, minimum_epoch)?;
        fallback_epoch.passive_book_fallback =
            Some(crate::strategy::hedged_grid::PassiveBookFallbackAnchor {
                fill_id: fill_id.clone(),
                fill_price: *fill_price,
                anchor_price: fallback_epoch.anchor_price,
                crossing_side,
                crossing_limit_price,
                bid,
                ask,
                selected_at_ms: wall_clock_ms()?,
            });
        fallback_epoch.validate(checkpoint.state.params.grid_count)?;
        plan = epoch_install_plan(checkpoint, venue, binding, fallback_epoch, bid, ask)?;
        if let Some((side, limit_price)) = plan.crossing {
            log_passive_wait(side, limit_price, bid, ask);
            return Ok(());
        }
        info!(
            event = "stage7_grid_install_passive_book_fallback_selected",
            fill_id,
            fill_price = %fill_price.value(),
            fallback_anchor = %plan.state.epoch.as_ref().ok_or(Stage7GridError::Command)?.anchor_price.value(),
            bid = %bid.value(),
            ask = %ask.value(),
            "成交重心会使完整 post-only 网格穿价；WAL 前持久化最新 BBO 中点回退"
        );
    }

    let opening_long = plan
        .opening
        .iter()
        .filter(|mutation| matches!(mutation, Stage7Mutation::Place(command) if command.position_side == PositionSide::Long))
        .count();
    let opening_short = plan
        .opening
        .iter()
        .filter(|mutation| matches!(mutation, Stage7Mutation::Place(command) if command.position_side == PositionSide::Short))
        .count();
    let configured_grid_count = usize::from(checkpoint.state.params.grid_count);
    if opening_long != configured_grid_count
        || opening_short != configured_grid_count
        || (!checkpoint
            .state
            .suppress_replenishment_until_inventory_recovers
            && inventory_low(&checkpoint.state, inventory))
    {
        return Err(Stage7GridError::Inventory);
    }
    checkpoint.state = plan.state;
    save_checkpoint(store, checkpoint)?;
    for mutation in plan.closing.iter().chain(plan.opening.iter()) {
        mutation.prepare(commands)?;
    }
    match execute_mutations(commands, venue, authority, writer, plan.closing, false) {
        Ok(()) => {}
        Err(Stage7GridError::Rejected | Stage7GridError::Unresolved) => {
            retire_undispatched_openings(commands, &plan.opening)?;
            defer_failed_epoch_install(checkpoint, store)?;
            return Ok(());
        }
        Err(error) => return Err(error),
    }
    // A signed acknowledgement for the closing wave can outlive the public freshness window.
    // Persist the frames captured during that wait before checking the opening wave. This grants
    // no writer or risk authority; the refreshed BBO must still prove every opening is passive.
    let _ = refresh_public(venue)?;
    let opening_now_ms = wall_clock_ms()?;
    let opening_book = venue.best_bid_ask(opening_now_ms);
    let opening_is_passive = opening_book.as_ref().is_ok_and(|(opening_bid, opening_ask)| {
        plan.opening.iter().all(|mutation| {
            matches!(mutation, Stage7Mutation::Place(command) if post_only_is_passive(command, *opening_bid, *opening_ask))
        })
    });
    if !opening_is_passive {
        retire_undispatched_openings(commands, &plan.opening)?;
        defer_failed_epoch_install(checkpoint, store)?;
        match opening_book {
            Ok((opening_bid, opening_ask)) => info!(
                event = "stage7_grid_install_opening_waiting_for_passive_book",
                bid = %opening_bid.value(),
                ask = %opening_ask.value(),
                "closing 波次完成后盘口已移动；opening 未发送并转签名重建"
            ),
            Err(error) => info!(
                event = "stage7_grid_install_opening_waiting_for_passive_book",
                reason = %error,
                "closing 波次完成后盘口不可用；opening 未发送并转签名重建"
            ),
        }
        return Ok(());
    }
    match execute_mutations(commands, venue, authority, writer, plan.opening, false) {
        Ok(()) => Ok(()),
        Err(Stage7GridError::Rejected | Stage7GridError::Unresolved) => {
            defer_failed_epoch_install(checkpoint, store)
        }
        Err(error) => Err(error),
    }
}

fn epoch_install_plan<V: HedgedGridVenue>(
    checkpoint: &Stage7GridCheckpoint,
    venue: &V,
    binding: &HedgedGridBinding,
    epoch: GridEpoch,
    bid: Price,
    ask: Price,
) -> Result<EpochInstallPlan, Stage7GridError> {
    assert_grid_order_notional(
        epoch.grid_quantity,
        epoch.anchor_price,
        venue.instrument(),
        binding,
        checkpoint.state.params.grid_count,
    )?;
    let mut state = checkpoint.state.clone();
    let GridDecision::Actions(actions) = state.install_epoch(epoch)? else {
        return Err(Stage7GridError::Strategy(HedgedGridError::Phase));
    };
    let mut closing = Vec::new();
    let mut opening = Vec::new();
    let mut crossing = None;
    for action in actions {
        let GridAction::Place(intent) = action else {
            return Err(Stage7GridError::Strategy(HedgedGridError::Phase));
        };
        let mutation =
            Stage7Mutation::from_grid(place_command(binding, venue.instrument(), &intent)?);
        let Stage7Mutation::Place(command) = &mutation else {
            return Err(Stage7GridError::Command);
        };
        if crossing.is_none() && !post_only_is_passive(command, bid, ask) {
            crossing = Some((command.side, command.limit_price));
        }
        assert_grid_order_notional(
            command.quantity,
            command.limit_price,
            venue.instrument(),
            binding,
            state.params.grid_count,
        )?;
        if intent.reduce_only {
            closing.push(mutation);
        } else {
            opening.push(mutation);
        }
    }
    Ok(EpochInstallPlan {
        state,
        closing,
        opening,
        crossing,
    })
}

fn post_only_is_passive(command: &crate::domain::OrderCommand, bid: Price, ask: Price) -> bool {
    match command.side {
        OrderSide::Buy => command.limit_price.value() < ask.value(),
        OrderSide::Sell => command.limit_price.value() > bid.value(),
    }
}

fn log_passive_wait(side: OrderSide, limit_price: Price, bid: Price, ask: Price) {
    info!(
        event = "stage7_grid_install_waiting_for_passive_book",
        ?side,
        limit_price = %limit_price.value(),
        bid = %bid.value(),
        ask = %ask.value(),
        "完整 epoch 仍含穿价 post-only 意图；保持新增风险关闭并等待新盘口"
    );
}

fn retire_undispatched_openings(
    commands: &mut CommandJournal,
    opening: &[Stage7Mutation],
) -> Result<(), Stage7GridError> {
    for mutation in opening {
        if commands
            .receipt(mutation.command_id())
            .is_some_and(|receipt| matches!(receipt.state, CommandState::Prepared))
        {
            commands.transition(
                mutation.command_id(),
                CommandState::Rejected {
                    reason: "epoch install aborted before opening dispatch".to_owned(),
                },
            )?;
        }
    }
    Ok(())
}

fn defer_failed_epoch_install(
    checkpoint: &mut Stage7GridCheckpoint,
    store: &ProjectionStore,
) -> Result<(), Stage7GridError> {
    checkpoint.state.block_for_order_reconciliation()?;
    let not_before_ms = wall_clock_ms()?.saturating_add(REJECTED_GRID_RESET_DELAY_MS);
    checkpoint
        .state
        .defer_blocked_reconciliation_until(not_before_ms)?;
    save_checkpoint(store, checkpoint)?;
    warn!(
        event = "stage7_grid_install_rejected_reset_deferred",
        not_before_ms,
        delay_ms = REJECTED_GRID_RESET_DELAY_MS,
        "网格安装未全部成功，保持新增风险关闭，30秒后按签名订单事实重建"
    );
    Ok(())
}
