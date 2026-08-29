use super::*;

#[derive(Default)]
pub(super) struct Stage7StreamFillAccumulator {
    by_order: BTreeMap<GridOrderKey, BTreeMap<String, GridVenueFill>>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct FillDriveOutcome {
    pub(super) mutation_dispatched: bool,
    pub(super) private_reconcile_required: bool,
    pub(super) wait_for_fresh_book: bool,
    pub(super) recenter_required: bool,
}

impl FillDriveOutcome {
    pub(super) const fn private_readback() -> Self {
        Self {
            private_reconcile_required: true,
            ..Self::idle()
        }
    }

    pub(super) const fn dispatched() -> Self {
        Self {
            mutation_dispatched: true,
            private_reconcile_required: true,
            wait_for_fresh_book: false,
            recenter_required: false,
        }
    }

    pub(super) const fn failed_closed() -> Self {
        Self {
            mutation_dispatched: false,
            private_reconcile_required: true,
            wait_for_fresh_book: false,
            recenter_required: false,
        }
    }

    pub(super) const fn recenter() -> Self {
        Self {
            mutation_dispatched: false,
            private_reconcile_required: true,
            wait_for_fresh_book: false,
            recenter_required: true,
        }
    }

    pub(super) const fn idle() -> Self {
        Self {
            mutation_dispatched: false,
            private_reconcile_required: false,
            wait_for_fresh_book: false,
            recenter_required: false,
        }
    }
}

impl Stage7StreamFillAccumulator {
    pub(super) fn clear(&mut self) {
        self.by_order.clear();
    }
}

pub(super) fn recover_interrupted_fill_transactions(
    checkpoint: &mut Stage7GridCheckpoint,
    commands: &CommandJournal,
) -> Result<bool, Stage7GridError> {
    if checkpoint.state.pending_transactions.is_empty()
        || checkpoint.state.phase != GridPhase::Running
    {
        return Ok(false);
    }
    let transaction_ids = checkpoint
        .state
        .pending_transactions
        .values()
        .map(|transaction| transaction.id.clone())
        .collect::<Vec<_>>();
    // dispatch_fill_actions orders every replacement before its cancel and execute_mutations
    // prepares that exact vector from front to back. Absence of every replacement identity
    // therefore also proves that no cancel from the candidate batch reached the WAL.
    let any_replacement_reached_wal = checkpoint
        .state
        .pending_transactions
        .values()
        .flat_map(|transaction| transaction.places.iter())
        .map(|intent| client_order_id(&intent.key))
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|client_id| commands.command_id_by_client_id(client_id).is_some());
    if any_replacement_reached_wal {
        // At least one child identity crossed the durable boundary, so restoring the old cancel
        // target as if the batch never existed would be false. Keep the optimistic transaction
        // fenced until exact WAL outcomes and a complete signed order surface reconstruct it.
        for transaction_id in transaction_ids {
            let _ = checkpoint
                .state
                .settle_transaction(&transaction_id, false)?;
        }
        return Ok(true);
    }
    let _ = checkpoint
        .state
        .abandon_unsubmitted_transactions_for_reconciliation(&transaction_ids)?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn process_stream_grid_fill<V>(
    checkpoint: &mut Stage7GridCheckpoint,
    store: &ProjectionStore,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    accumulator: &mut Stage7StreamFillAccumulator,
    record: GridVenueFill,
    event_generation: u64,
    received_at_ms: u64,
) -> Result<FillDriveOutcome, Stage7GridError>
where
    V: HedgedGridVenue,
{
    let FieldState::Known(client_order_id) = &record.client_order_id else {
        return Ok(FillDriveOutcome::private_readback());
    };
    let Ok(key) = parse_grid_client_order_id(client_order_id) else {
        return Ok(FillDriveOutcome::private_readback());
    };
    let Some(expected) = checkpoint.state.owned_orders.get(&key).cloned() else {
        return Ok(FillDriveOutcome::private_readback());
    };
    if commands
        .client_id_by_venue_order_id(&record.fill.order_id)
        .is_some_and(|durable_client_id| durable_client_id.as_str() != client_order_id)
    {
        return Err(Stage7GridError::FillLiquidityUnknown);
    }
    let fills = accumulator.by_order.entry(key.clone()).or_default();
    match fills.get(&record.fill.fill_id) {
        Some(existing) if existing == &record => {}
        Some(_) => return Err(Stage7GridError::FillLiquidityUnknown),
        None => {
            fills.insert(record.fill.fill_id.clone(), record.clone());
        }
    }
    let candidates = fills
        .values()
        .map(|candidate| &candidate.fill)
        .collect::<Vec<_>>();
    let terminal =
        match super::super::hedged_grid::terminal_owned_execution(&candidates, expected.quantity) {
            Ok(terminal) => terminal.clone(),
            Err(super::super::hedged_grid::TerminalExecutionError::IncompleteQuantity) => {
                return Ok(FillDriveOutcome::idle());
            }
            Err(_) => return Err(Stage7GridError::FillLiquidityUnknown),
        };
    accumulator.by_order.remove(&key);

    let private_generation =
        stream_fill_private_generation(&checkpoint.state, event_generation, received_at_ms)?;
    let mut planned = checkpoint.clone();
    let application = match super::super::hedged_grid::apply_owned_grid_fill(
        &mut planned.state,
        crate::strategy::hedged_grid::OwnedGridFill {
            fill_id: terminal.fill_id.clone(),
            private_generation,
            source_order: key,
            fill_price: terminal.price,
            complete: true,
            maker: terminal.maker,
        },
        super::super::hedged_grid::GridFillProjection::ProjectStreamInventory,
    ) {
        Ok(application) => application,
        Err(HedgedGridError::Rolling) => {
            request_reconciliation_reset_unless_batch_is_blocked(&mut checkpoint.state)?;
            save_checkpoint(store, checkpoint)?;
            return Ok(FillDriveOutcome::recenter());
        }
        Err(error) => return Err(error.into()),
    };
    let actions = match application {
        super::super::hedged_grid::GridFillApplication::Rolling(actions) => actions,
        super::super::hedged_grid::GridFillApplication::ReanchorPending => {
            save_checkpoint(store, &planned)?;
            *checkpoint = planned;
            return Ok(FillDriveOutcome::private_readback());
        }
        super::super::hedged_grid::GridFillApplication::Noop => return Ok(FillDriveOutcome::idle()),
        super::super::hedged_grid::GridFillApplication::TakerInventoryOnly => {
            return Err(Stage7GridError::PostOnlyFillBecameTaker);
        }
        super::super::hedged_grid::GridFillApplication::AwaitLiquidityEvidence => {
            return Err(Stage7GridError::FillLiquidityUnknown);
        }
    };
    if rolling_actions_exceed_order_cap(
        &actions,
        venue.instrument(),
        binding,
        planned.state.params.grid_count,
    )? {
        let transaction_ids = rolling_transaction_ids(&actions);
        let _ = planned
            .state
            .abandon_unsubmitted_transactions_for_reconciliation(&transaction_ids)?;
        request_reconciliation_reset_unless_batch_is_blocked(&mut planned.state)?;
        save_checkpoint(store, &planned)?;
        *checkpoint = planned;
        return Ok(FillDriveOutcome::recenter());
    }

    save_checkpoint(store, &planned)?;
    *checkpoint = planned;
    let fill_drive_started_at_ms = wall_clock_ms()?;
    let outcome = dispatch_fill_actions(
        checkpoint, commands, venue, authority, writer, binding, actions, store,
    )?;
    save_checkpoint(store, checkpoint)?;
    if outcome.mutation_dispatched {
        info!(
            event = "stage7_stream_fill_dispatched",
            exchange = venue.exchange(),
            fill_id = %terminal.fill_id,
            event_time_ms = terminal.exchange_time_ms.unwrap_or(0),
            received_at_ms,
            fill_drive_started_at_ms,
            event_to_fill_drive_ms = fill_drive_started_at_ms
                .saturating_sub(terminal.exchange_time_ms.unwrap_or(received_at_ms)),
            received_to_fill_drive_ms = fill_drive_started_at_ms.saturating_sub(received_at_ms),
            "完整用户流成交已在签名REST前直接进入post-only补撤dispatch"
        );
    }
    Ok(outcome)
}

fn stream_fill_private_generation(
    state: &HedgedGridState,
    event_generation: u64,
    received_at_ms: u64,
) -> Result<u64, Stage7GridError> {
    if event_generation == 0 || received_at_ms == 0 {
        return Err(Stage7GridError::PrivateEvidence);
    }
    let Some(inventory) = state.inventory.as_ref() else {
        return Ok(event_generation);
    };
    if received_at_ms <= inventory.private_observed_at_ms {
        return Ok(inventory.private_generation.max(event_generation));
    }
    inventory
        .private_generation
        .checked_add(1)
        .map(|generation| generation.max(event_generation))
        .ok_or(Stage7GridError::Clock)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn process_complete_owned_fills<V>(
    checkpoint: &mut Stage7GridCheckpoint,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
    store: &ProjectionStore,
) -> Result<FillDriveOutcome, Stage7GridError>
where
    V: HedgedGridVenue,
{
    // A signed page may contain several retired fills and an older partial fill at the same time.
    // Plan complete executions in venue sequence on a checkpoint copy, but never let one
    // incomplete order starve independent complete executions that follow it.
    let mut planned = checkpoint.clone();
    let mut actions = Vec::new();
    let mut seen = BTreeSet::new();
    let mut processed_orders = BTreeSet::new();
    let mut fills = Vec::new();
    let resolved_fills = resolve_grid_fill_client_ids(commands, &readback.fills);
    for record in &resolved_fills {
        let FieldState::Known(client_order_id) = &record.client_order_id else {
            continue;
        };
        let Ok(key) = parse_grid_client_order_id(client_order_id) else {
            continue;
        };
        if !planned.state.owned_orders.contains_key(&key) {
            continue;
        }
        match super::super::hedged_grid::route_grid_fill(&record.fill) {
            super::super::hedged_grid::GridFillRoute::MakerDrive => fills.push((record, key)),
            super::super::hedged_grid::GridFillRoute::TakerInventoryOnly => {
                return Err(Stage7GridError::PostOnlyFillBecameTaker);
            }
            super::super::hedged_grid::GridFillRoute::AwaitLiquidityEvidence => {
                return Err(Stage7GridError::FillLiquidityUnknown);
            }
        }
    }
    // One signed page can retire several owned orders. The venue-native execution sequence is
    // the only authority for which completion happened first; role, map order, timestamp and
    // opaque fill id must never choose which fill consumes AwaitingNextOwnedFill.
    sort_grid_fill_candidates_by_execution_sequence(&mut fills)?;
    for (record, key) in fills {
        if processed_orders.contains(&key) || !planned.state.owned_orders.contains_key(&key) {
            continue;
        }
        let FieldState::Known(client_order_id) = &record.client_order_id else {
            continue;
        };
        let expected_quantity = planned
            .state
            .owned_orders
            .get(&key)
            .map(|order| order.quantity)
            .ok_or(Stage7GridError::Inventory)?;
        let matching_fills = resolved_fills
            .iter()
            .filter(|candidate| {
                matches!(
                    &candidate.client_order_id,
                    FieldState::Known(candidate_id) if candidate_id == client_order_id
                )
            })
            .map(|candidate| &candidate.fill)
            .collect::<Vec<_>>();
        let terminal_fill = match super::super::hedged_grid::terminal_owned_execution(
            &matching_fills,
            expected_quantity,
        ) {
            Ok(fill) => fill,
            Err(super::super::hedged_grid::TerminalExecutionError::IncompleteQuantity) => {
                // The private stream accumulator owns the next fragment; if that transport is
                // unavailable, the ordinary bounded periodic readback will retry. A partial fill
                // must not turn a full account-history query into an unbounded busy loop.
                continue;
            }
            Err(super::super::hedged_grid::TerminalExecutionError::Liquidity)
                if matching_fills.iter().any(|fill| {
                    matches!(
                        super::super::hedged_grid::route_grid_fill(fill),
                        super::super::hedged_grid::GridFillRoute::TakerInventoryOnly
                    )
                }) =>
            {
                return Err(Stage7GridError::PostOnlyFillBecameTaker);
            }
            Err(_) => return Err(Stage7GridError::FillLiquidityUnknown),
        };
        if record.fill.fill_id != terminal_fill.fill_id {
            continue;
        }
        if !seen.insert(record.fill.fill_id.clone()) {
            return Err(Stage7GridError::FillLiquidityUnknown);
        }
        processed_orders.insert(key.clone());
        // The complete signed fill page already binds one owned client identity, unique
        // execution id, full accumulated quantity, terminal maker evidence and current signed
        // inventory. A second per-order REST query adds latency but no stronger authority.
        match super::super::hedged_grid::apply_owned_grid_fill(
            &mut planned.state,
            crate::strategy::hedged_grid::OwnedGridFill {
                fill_id: record.fill.fill_id.clone(),
                private_generation: planned.private_generation,
                source_order: key.clone(),
                fill_price: record.fill.price,
                complete: true,
                maker: record.fill.maker.clone(),
            },
            super::super::hedged_grid::GridFillProjection::SignedInventoryIncluded,
        ) {
            Ok(super::super::hedged_grid::GridFillApplication::Rolling(mut next)) => {
                actions.append(&mut next);
                if rolling_actions_exceed_order_cap(
                    &actions,
                    venue.instrument(),
                    binding,
                    planned.state.params.grid_count,
                )? {
                    let transaction_ids = rolling_transaction_ids(&actions);
                    let _ = planned
                        .state
                        .abandon_unsubmitted_transactions_for_reconciliation(&transaction_ids)?;
                    info!(
                        event = "stage7_grid_rolling_regrid",
                        exchange = venue.exchange(),
                        reason = "replacement_would_exceed_fixed_order_cap",
                        "滚动网格在提交前触及单笔上限，转入签名对账重建"
                    );
                    save_checkpoint(store, &planned)?;
                    *checkpoint = planned;
                    // The fill is already an exact signed fact. Rebuild from a new private
                    // snapshot rather than submitting a smaller replacement or changing the
                    // reducer's fixed quantity.
                    return Ok(FillDriveOutcome::recenter());
                }
            }
            Ok(super::super::hedged_grid::GridFillApplication::ReanchorPending) => {
                if !actions.is_empty() {
                    return Err(Stage7GridError::Strategy(HedgedGridError::Phase));
                }
                save_checkpoint(store, &planned)?;
                *checkpoint = planned;
                return Ok(FillDriveOutcome::private_readback());
            }
            Ok(super::super::hedged_grid::GridFillApplication::Noop) => {}
            Ok(super::super::hedged_grid::GridFillApplication::TakerInventoryOnly) => {
                return Err(Stage7GridError::PostOnlyFillBecameTaker);
            }
            Ok(super::super::hedged_grid::GridFillApplication::AwaitLiquidityEvidence) => {
                return Err(Stage7GridError::FillLiquidityUnknown);
            }
            // Several same-lane fills can exhaust the opposite-role cancel capacity in one
            // signed batch. Preserve every earlier successful 2-place/1-cancel transaction,
            // then rebuild the symbol from signed inventory. Rolling is an expected
            // reconciliation boundary for either role, never a reason to terminate resident.
            Err(HedgedGridError::Rolling) => {
                let outcome = dispatch_fill_actions(
                    &mut planned,
                    commands,
                    venue,
                    authority,
                    writer,
                    binding,
                    actions,
                    store,
                )?;
                request_reconciliation_reset_unless_batch_is_blocked(&mut planned.state)?;
                save_checkpoint(store, &planned)?;
                *checkpoint = planned;
                return Ok(FillDriveOutcome {
                    private_reconcile_required: true,
                    recenter_required: !outcome.wait_for_fresh_book,
                    ..outcome
                });
            }
            Err(error) => return Err(error.into()),
        }
    }
    let outcome = dispatch_fill_actions(
        &mut planned,
        commands,
        venue,
        authority,
        writer,
        binding,
        actions,
        store,
    )?;
    save_checkpoint(store, &planned)?;
    *checkpoint = planned;
    Ok(outcome)
}
