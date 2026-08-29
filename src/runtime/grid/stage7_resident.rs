use crate::runtime::hedged_grid;

use super::*;

pub(super) fn run_stage7_grid<V: HedgedGridVenue>(
    cfg: &Config,
    request: Stage7GridRequest,
    binding: HedgedGridBinding,
    venue: &mut V,
) -> Result<Stage7GridReport, Stage7GridError> {
    if request.shadow_only {
        return run_stage7_grid_shadow(cfg, request, binding, venue);
    }
    let writer_scope = stage7_writer_scope(&binding);
    let _canonical_root_guard = acquire_stage7_writer_root(&writer_scope, &request.artifacts_root)?;
    fs::create_dir_all(&request.artifacts_root).map_err(|source| Stage7GridError::Io {
        path: request.artifacts_root.clone(),
        source,
    })?;
    let params = release_params(cfg, &binding)?;
    let control_store = ProjectionStore::new(request.artifacts_root.join(CONTROL_FILE));
    let checkpoint_path = request.artifacts_root.join(CHECKPOINT_FILE);
    let checkpoint_store = ProjectionStore::new(&checkpoint_path);
    initialize_control_for_new_root(&control_store, &checkpoint_store, &binding)?;
    let mut checkpoint =
        load_checkpoint(&checkpoint_store, &binding, &params, request.reset_on_start)?;
    let exposure_settings = stage7_exposure::initialize_exposure_guard(
        cfg,
        &binding,
        &mut checkpoint,
        &request.artifacts_root,
    )?;
    venue.set_fill_history_start_ms(checkpoint.fill_history_start_ms);
    if request.skip_inventory_replenishment_until_recovered
        && !matches!(
            checkpoint.state.phase,
            GridPhase::BlockedUnknown | GridPhase::Stopping
        )
    {
        checkpoint.state.request_restart_without_replenishment()?;
    } else if request.reset_on_start
        && !matches!(
            checkpoint.state.phase,
            GridPhase::BlockedUnknown | GridPhase::Stopping
        )
    {
        let _ = checkpoint.state.request_reset(GridResetReason::Manual)?;
    }
    save_checkpoint(&checkpoint_store, &checkpoint)?;
    let mut commands = CommandJournal::open(request.artifacts_root.join(COMMAND_FILE))?;
    // A restarted process never revives a pre-network Prepared row. Submitted rows become
    // Unknown and may settle only through their exact durable identity and signed readback.
    let _ = commands.fence_interrupted_dispatches()?;
    if recover_interrupted_fill_transactions(&mut checkpoint, &commands)? {
        // The reducer checkpoint is intentionally durable before the final public/WAL gate. If a
        // crash happened in that gap, exact WAL absence restores the cancel target; any WAL-bound
        // child instead fences the whole transaction until a signed projection reconciles it.
        save_checkpoint(&checkpoint_store, &checkpoint)?;
    }
    stage7_exposure::recover_unjournaled_exposure(&mut checkpoint, &checkpoint_store, &commands)?;
    let mut evidence = open_stage7_private_evidence(&request.artifacts_root, &binding)?;
    let mut exposure_shadow_evidence = hedged_grid::ExposureShadowEvidenceJournal::open(
        request
            .artifacts_root
            .join(hedged_grid::EXPOSURE_SHADOW_EVIDENCE_FILE),
    )?;
    let mut public_market = Stage7PublicRuntime::open(&request.artifacts_root, &binding)?;
    let mut risk_lane = Stage7RiskLane::new(venue.risk_readback_client());
    let authority =
        WriterLeaseAuthority::open(request.artifacts_root.join(WRITER_FILE), writer_scope)?;
    let mut writer = None;
    let mut turns = 0_u64;
    let mut needs_readback = true;
    let mut next_private_readback_ms = 0_u64;
    let mut next_risk_snapshot_ms = 0_u64;
    let mut private_stream_connected = false;
    let mut force_order_health_check = request.force_order_health_check;
    let mut retry_instrument_rules = false;
    let mut stream_fills = Stage7StreamFillAccumulator::default();

    loop {
        if request.max_turns.is_some_and(|limit| turns >= limit) {
            break;
        }
        if let Some(deadline_ms) = request.wall_clock_deadline_ms
            && wall_clock_ms()? >= deadline_ms
        {
            break;
        }
        turns = turns.checked_add(1).ok_or(Stage7GridError::Clock)?;
        let control = read_control(&control_store, &binding)?;
        if control == HedgedGridControlTarget::Stop {
            retry_instrument_rules = false;
            if !request.shadow_only && checkpoint.state.phase != GridPhase::Stopping {
                checkpoint.state.phase = GridPhase::Stopping;
                save_checkpoint(&checkpoint_store, &checkpoint)?;
            }
            needs_readback = true;
        } else if control == HedgedGridControlTarget::Reset
            && !matches!(
                checkpoint.state.phase,
                GridPhase::BlockedUnknown | GridPhase::Stopping
            )
        {
            if checkpoint.state.phase != GridPhase::ResettingGrid {
                let _ = checkpoint.state.request_reset(GridResetReason::Manual)?;
                save_checkpoint(&checkpoint_store, &checkpoint)?;
            }
            control_store.save(&Stage7GridControl {
                schema_version: 1,
                binding: binding.clone(),
                target: HedgedGridControlTarget::Running,
            })?;
            needs_readback = true;
        }

        if let Err(error) = venue.connect_private_stream() {
            // A private WebSocket never authorizes mutation. The signed readback loop remains
            // authoritative while the next iteration retries the stream setup.
            warn!(
                event = "stage7_private_stream_unavailable",
                exchange = venue.exchange(),
                reason = %error,
                "private stream setup failed; retaining signed REST reconciliation"
            );
            needs_readback = true;
        } else {
            private_stream_connected = true;
            let drain_result = (|| {
                // A busy account stream must not monopolize a resident turn. The following
                // signed readback is still required before mutation, while this bound lets stop,
                // Canary deadlines and periodic health checks make progress under bursty events.
                for _ in 0..MAX_PRIVATE_EVENTS_PER_TURN {
                    let Some(event) = venue.next_private_event()? else {
                        break;
                    };
                    let received_at_ms = wall_clock_ms()?;
                    let event_generation = evidence
                        .last_generation()
                        .max(checkpoint.private_generation)
                        .checked_add(1)
                        .ok_or(Stage7GridError::Clock)?;
                    append_private_payload(
                        &mut evidence,
                        event_generation,
                        received_at_ms,
                        event_payload(&event),
                    )?;
                    match event {
                        GridPrivateEvent::Fill {
                            fill,
                            client_order_id,
                            ..
                        } if !request.shadow_only
                            && control == HedgedGridControlTarget::Running
                            && checkpoint.state.phase == GridPhase::Running
                            && checkpoint.pending_exposure_reduction.is_none()
                            && !checkpoint.order_health_fenced
                            && !retry_instrument_rules
                            && !commands.has_unresolved()
                            && writer.is_some() =>
                        {
                            let fill_needs_readback = process_stream_grid_fill(
                                &mut checkpoint,
                                &checkpoint_store,
                                &mut commands,
                                venue,
                                &authority,
                                writer.as_ref().ok_or(Stage7GridError::Writer)?,
                                &binding,
                                &mut stream_fills,
                                GridVenueFill {
                                    fill,
                                    client_order_id,
                                },
                                event_generation,
                                received_at_ms,
                            )?;
                            if !fill_needs_readback.wait_for_fresh_book {
                                needs_readback |= fill_needs_readback.private_reconcile_required;
                            }
                            if fill_needs_readback.recenter_required {
                                info!(
                                    event = "stage7_stream_fill_recenter_required",
                                    exchange = venue.exchange(),
                                    "用户流成交替代价已越过新盘口；立即转签名重建"
                                );
                            }
                            if fill_needs_readback.private_reconcile_required {
                                break;
                            }
                        }
                        GridPrivateEvent::Fill { .. } | GridPrivateEvent::Reconcile { .. } => {
                            needs_readback = true;
                        }
                    }
                }
                Ok::<(), Stage7GridError>(())
            })();
            if let Err(error) = drain_result {
                // A failed stream can only be replaced by a later connection plus a strictly
                // newer signed readback. Retaining an old socket or its buffered events would
                // otherwise allow it to cross the recovery generation boundary.
                warn!(
                    event = "stage7_private_stream_reset",
                    exchange = venue.exchange(),
                    reason = %error,
                    "private stream generation failed; fencing it before REST recovery"
                );
                venue.reset_private_stream();
                needs_readback = true;
            }
        }

        let mut public_ready = public_market.drive(venue, wall_clock_ms()?)?;
        // A busy public socket may take longer than the resident turn timestamp to drain.  All
        // freshness and subsequent mutation gates must use a clock sampled after that durable
        // public boundary, never the time before it.
        let mut now_ms = wall_clock_ms()?;

        if checkpoint.state.phase == GridPhase::ResettingGrid && public_ready {
            // ResettingGrid is durable across process restarts, unlike an in-memory wake flag. A
            // public outage must not hammer signed REST, but its first recovered durable book must
            // wake a new signed generation immediately instead of waiting for the ten-minute
            // periodic watermark. The reset branches below remain the only mutation authority.
            needs_readback = true;
        }
        let latched_repair_pending = stage7_exposure::latched_exposure_repair_pending(&checkpoint)?;
        if public_ready && latched_repair_pending {
            // Public-deferred risk repair deliberately stops signed REST polling while the book
            // is unavailable. Its first recovered durable book wakes a newer private generation;
            // the old signed view never authorizes the repair.
            needs_readback = true;
        }

        if scheduled_private_readback_allowed(public_ready, latched_repair_pending) {
            if now_ms >= next_private_readback_ms {
                needs_readback = true;
            }
            if force_order_health_check || order_health_due(&checkpoint, now_ms) {
                needs_readback = true;
            }
        }
        if !needs_readback
            && poll_live_exposure_if_due(
                exposure_settings.as_ref(),
                &mut checkpoint,
                &checkpoint_store,
                &mut commands,
                &mut evidence,
                &mut exposure_shadow_evidence,
                venue,
                &authority,
                &mut writer,
                &binding,
                request.shadow_only,
                control,
                now_ms,
                &mut next_risk_snapshot_ms,
                &mut risk_lane,
            )?
        {
            needs_readback = true;
            continue;
        }
        if retry_instrument_rules && checkpoint.state.phase != GridPhase::Stopping {
            match venue.verify_current_instrument_rules() {
                Ok(()) => {
                    retry_instrument_rules = false;
                    // Resume only after a new complete private view. The readback that preceded
                    // the outage cannot authorize mutations after the rule endpoint recovers.
                    needs_readback = true;
                }
                Err(error) if is_transient_instrument_rule_error(&error) => {
                    warn!(
                        event = "stage7_instrument_rules_backoff",
                        exchange = venue.exchange(),
                        reason = %error,
                        "current instrument rules are temporarily unavailable; retaining the closed mutation gate"
                    );
                    thread::sleep(Duration::from_millis(250));
                    continue;
                }
                Err(error) => {
                    warn!(
                        event = "stage7_instrument_rules_fenced",
                        exchange = venue.exchange(),
                        reason = %error,
                        "current instrument rules differ from the admitted release; stopping this binding"
                    );
                    retry_instrument_rules = false;
                    checkpoint.state.phase = GridPhase::Stopping;
                    control_store.save(&Stage7GridControl {
                        schema_version: 1,
                        binding: binding.clone(),
                        target: HedgedGridControlTarget::Stop,
                    })?;
                    save_checkpoint(&checkpoint_store, &checkpoint)?;
                    needs_readback = true;
                    continue;
                }
            }
        }
        if !needs_readback {
            thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
            continue;
        }

        let readback = match venue.readback() {
            Ok(readback) => readback,
            Err(error) if is_transient_readback_error(&error) => {
                // A transient signed-readback transport fault must not exit a resident that may
                // still own orders.  It remains fenced, preserves its writer/WAL and retries a
                // complete fresh private view before any later mutation.
                warn!(
                    event = "stage7_private_readback_backoff",
                    exchange = venue.exchange(),
                    reason = %error,
                    "private readback failed transiently; retaining the closed mutation gate"
                );
                needs_readback = true;
                thread::sleep(Duration::from_millis(250));
                continue;
            }
            Err(error) => {
                // Preserve the precise signed-readback boundary in the resident log.  The
                // returned error remains fail-closed, but a low-level venue payload error is
                // otherwise indistinguishable from a public-book error at the CLI boundary.
                warn!(
                    event = "stage7_private_readback_failed",
                    exchange = venue.exchange(),
                    reason = %error,
                    "private signed readback failed; terminating before any further mutation"
                );
                return Err(error.into());
            }
        };
        require_complete_order_family_readback(&readback)?;
        require_no_unmanaged_order_family_rows(&readback)?;
        // The complete signed fill page supersedes any partial per-order stream accumulation.
        // Keeping fragments across this boundary could combine two transport generations.
        stream_fills.clear();
        if !request.shadow_only
            && let Some(active) = authority.active_session()?
        {
            // Legacy Binance used an epoch-millisecond writer readback watermark while Stage 7
            // uses a monotonic private generation. A fresh signed readback may rebase only above
            // that exact same-scope durable writer; it never elects a replacement writer.
            checkpoint.private_generation = checkpoint
                .private_generation
                .max(active.readback_generation);
        }
        let generation = record_readback(&mut evidence, &checkpoint, now_ms, &readback)?;
        checkpoint.private_generation = generation;
        if request.shadow_only {
            // Shadow commonly observes the same account while a separately admitted predecessor
            // still owns live orders. Those orders are evidence to inspect, not identities this
            // mutation-free root may reconcile or adopt. Persist the complete signed generation,
            // preview the shared reducer from authoritative inventory, and evaluate the shared
            // exposure guard without entering writer/WAL/order-ownership paths.
            if public_ready && let Ok((bid, ask)) = venue.best_bid_ask(now_ms) {
                let inventory =
                    inventory(&readback, generation, now_ms, bid, ask, &binding.symbol)?;
                let mut preview = checkpoint.state.clone();
                let decision = preview.observe_inventory(inventory)?;
                info!(
                    event = "stage7_grid_shadow_strategy_preview",
                    exchange = venue.exchange(),
                    generation,
                    phase = ?preview.phase,
                    decision = ?decision,
                    observed_orders = readback.orders.len(),
                    "共享网格影子已用签名库存执行纯 reducer 预览"
                );
            }
            if let Some(settings) = &exposure_settings
                && now_ms >= next_risk_snapshot_ms
            {
                let exposure_result = stage7_exposure::poll_exposure_take_profit(
                    settings,
                    &mut checkpoint,
                    &checkpoint_store,
                    &mut commands,
                    &mut evidence,
                    &mut exposure_shadow_evidence,
                    venue,
                    &authority,
                    &mut writer,
                    &binding,
                    true,
                    now_ms,
                    None,
                );
                if let Err(error) = exposure_result {
                    warn!(
                        event = "stage7_shadow_exposure_snapshot_failed_closed",
                        exchange = venue.exchange(),
                        reason = %error,
                        "影子风险快照失败关闭；保留已持久的私有事实并等待下一轮"
                    );
                }
                next_risk_snapshot_ms = now_ms.saturating_add(settings.snapshot_interval_ms);
            }
            save_checkpoint(&checkpoint_store, &checkpoint)?;
            info!(
                event = "stage7_grid_shadow_readback",
                exchange = venue.exchange(),
                generation = checkpoint.private_generation,
                phase = ?checkpoint.state.phase,
                observed_orders = readback.orders.len(),
                "阶段7只读影子网格已完成一轮签名对账"
            );
            thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
            continue;
        }
        if signed_readback_contains_settling_owned_cancel(
            &checkpoint.state,
            &commands,
            &binding,
            &readback,
        ) {
            info!(
                event = "stage7_signed_cancel_projection_settling",
                exchange = venue.exchange(),
                generation,
                "accepted owned cancel is still visible; waiting for a newer signed projection"
            );
            save_checkpoint(&checkpoint_store, &checkpoint)?;
            needs_readback = true;
            thread::sleep(Duration::from_millis(REJECTED_CANCEL_RETRY_MS));
            continue;
        }
        settle_signed_visible_order_receipts(
            &mut commands,
            &checkpoint.state,
            &binding,
            &readback,
        )?;
        if checkpoint.state.phase == GridPhase::Running
            && readback
                .all_order_families_empty()
                .map_err(|_| Stage7GridError::OrderFamily)?
            && !checkpoint.state.owned_orders.is_empty()
            && !checkpoint_projection_is_wal_bound(
                &checkpoint.state,
                &commands,
                &binding,
                venue.instrument(),
            )?
        {
            // A process can stop after persisting a candidate ladder but before its WAL prepare.
            // Only a signed empty order surface permits discarding that unbound projection. The
            // next install uses the WAL-wide epoch floor, so no historical identity is reused.
            checkpoint
                .state
                .begin_reconciliation_reset(BTreeMap::new())?;
            save_checkpoint(&checkpoint_store, &checkpoint)?;
            needs_readback = true;
            continue;
        }
        match checkpoint.state.phase {
            GridPhase::Stopping => {
                // Stop is risk-reducing and the complete signed surface is authoritative. A
                // failed rolling batch may have retired a still-live cancel target from the
                // optimistic checkpoint, so rebuild only from exact accepted place WAL before
                // issuing cancellations. Foreign or unresolved identities still fail closed.
                let owned = recovered_owned_orders(&commands, &binding, &readback)?;
                checkpoint.state.reconcile_stopping_orders(owned)?;
            }
            GridPhase::BlockedUnknown => {
                // Blocked recovery deliberately cannot trust its optimistic ladder. Prove the
                // whole visible surface against accepted WAL now; the due-watermark branch below
                // performs the actual projection transition after unresolved commands settle.
                let _ = recovered_owned_orders(&commands, &binding, &readback)?;
            }
            _ => {
                if reconcile_visible_order_drift(
                    &mut checkpoint.state,
                    &commands,
                    &binding,
                    &readback,
                )? {
                    // The signed exchange view is authoritative. If each visible order is
                    // nevertheless provably this writer's accepted command, reset from that view
                    // before any further mutation instead of misclassifying checkpoint lag as a
                    // foreign order.
                    save_checkpoint(&checkpoint_store, &checkpoint)?;
                    needs_readback = true;
                    continue;
                }
                verify_readback_scope(&checkpoint.state, &commands, &readback, &binding)?;
            }
        }
        save_checkpoint(&checkpoint_store, &checkpoint)?;
        next_private_readback_ms = now_ms.saturating_add(PRIVATE_READBACK_INTERVAL_MS);
        needs_readback = false;

        if checkpoint.order_health_fenced {
            // An explicit reset is the only recovery path: it is bound to this same artifacts
            // root and has already obtained the fresh signed readback above. A normal restart
            // cannot turn a prior unhealthy report into new risk.
            if control != HedgedGridControlTarget::Reset {
                return Err(Stage7GridError::OrderHealthFenced);
            }
            checkpoint.order_health_fenced = false;
            save_checkpoint(&checkpoint_store, &checkpoint)?;
        }

        writer = Some(active_writer(
            &authority,
            writer,
            wall_clock_ms()?,
            generation,
        )?);
        let already_stopping = checkpoint.state.phase == GridPhase::Stopping;
        let current_rules = if already_stopping {
            false
        } else if readback.balance.available_balance <= Decimal::ZERO {
            warn!(
                event = "stage7_available_margin_fenced",
                exchange = venue.exchange(),
                "signed available margin is not positive; rejecting prepared risk and stopping this binding"
            );
            false
        } else {
            match venue.verify_current_instrument_rules() {
                Ok(()) => true,
                Err(error) if is_transient_instrument_rule_error(&error) => {
                    warn!(
                        event = "stage7_instrument_rules_backoff",
                        exchange = venue.exchange(),
                        reason = %error,
                        "current instrument rules are temporarily unavailable; retaining the closed mutation gate"
                    );
                    retry_instrument_rules = true;
                    thread::sleep(Duration::from_millis(250));
                    continue;
                }
                Err(error) => {
                    warn!(
                        event = "stage7_instrument_rules_fenced",
                        exchange = venue.exchange(),
                        reason = %error,
                        "current instrument rules differ from the admitted release; stopping this binding"
                    );
                    checkpoint.state.phase = GridPhase::Stopping;
                    control_store.save(&Stage7GridControl {
                        schema_version: 1,
                        binding: binding.clone(),
                        target: HedgedGridControlTarget::Stop,
                    })?;
                    save_checkpoint(&checkpoint_store, &checkpoint)?;
                    needs_readback = true;
                    continue;
                }
            }
        };
        let _ = settle_due_interrupted_wal(
            control,
            &checkpoint.state,
            &mut commands,
            &binding,
            &readback,
            now_ms,
        )?;
        recover_unresolved(
            &mut commands,
            venue,
            &authority,
            writer.as_ref().ok_or(Stage7GridError::Writer)?,
            &binding,
            &readback,
            current_rules,
        )?;
        if !current_rules && !already_stopping {
            checkpoint.state.phase = GridPhase::Stopping;
            control_store.save(&Stage7GridControl {
                schema_version: 1,
                binding: binding.clone(),
                target: HedgedGridControlTarget::Stop,
            })?;
            save_checkpoint(&checkpoint_store, &checkpoint)?;
            needs_readback = true;
            continue;
        }
        if commands.has_unresolved() {
            if control != HedgedGridControlTarget::Stop
                && checkpoint.pending_exposure_reduction.is_none()
            {
                checkpoint.state.phase = GridPhase::BlockedUnknown;
            }
            save_checkpoint(&checkpoint_store, &checkpoint)?;
            needs_readback = true;
            continue;
        }

        if checkpoint.pending_exposure_reduction.is_some() {
            let settlement = stage7_exposure::settle_exposure_take_profit_with_public_refresh(
                &mut checkpoint,
                &checkpoint_store,
                &mut commands,
                venue,
                &authority,
                writer.as_ref().ok_or(Stage7GridError::Writer)?,
                &binding,
                &readback,
                &request.artifacts_root,
                generation,
                |venue| public_market.drive(venue, wall_clock_ms()?),
            )?;
            match settlement {
                stage7_exposure::ExposureSettlement::Complete => {}
                stage7_exposure::ExposureSettlement::PrivateReadbackRequired => {
                    needs_readback = true;
                    thread::sleep(Duration::from_millis(REJECTED_CANCEL_RETRY_MS));
                    continue;
                }
                stage7_exposure::ExposureSettlement::PublicDeferred => {
                    // Keep driving only the public lane until a fresh durable book wakes a new
                    // signed generation above. This avoids a 250ms full-account REST retry loop.
                    needs_readback = false;
                    thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
                    continue;
                }
            }
        }

        // Gate can retain a semantically fixed recovery anchor in ResettingGrid for an extended
        // period while waiting for a non-crossing book. A private-stream outage also forces every
        // turn through signed REST readback. In both cases the independent risk lane must still
        // sample equity/PnL and may submit only its reduce-only command before any grid mutation.
        if poll_live_exposure_if_due(
            exposure_settings.as_ref(),
            &mut checkpoint,
            &checkpoint_store,
            &mut commands,
            &mut evidence,
            &mut exposure_shadow_evidence,
            venue,
            &authority,
            &mut writer,
            &binding,
            request.shadow_only,
            control,
            now_ms,
            &mut next_risk_snapshot_ms,
            &mut risk_lane,
        )? {
            needs_readback = true;
            continue;
        }

        if checkpoint.state.blocked_reconciliation_is_due(now_ms) {
            // A terminal WAL receipt only proves that the request itself no longer needs
            // recovery. Reconstruct the open grid from the same signed readback before allowing
            // the reducer to leave BlockedUnknown; it must never reuse an optimistic projection.
            let owned = recovered_owned_orders(&commands, &binding, &readback)?;
            checkpoint.state.reconcile_blocked_orders(owned)?;
            save_checkpoint(&checkpoint_store, &checkpoint)?;
            needs_readback = true;
            continue;
        }
        if checkpoint.state.phase == GridPhase::BlockedUnknown {
            // A rejected/indeterminate physical batch keeps all new mutations fenced until its
            // durable 30-second settlement window expires. Schedule the next complete signed
            // readback at that watermark instead of falling through into ordinary inventory
            // transitions or hammering every private endpoint during the wait.
            if let Some(not_before_ms) = checkpoint.state.blocked_reconciliation_not_before_ms() {
                next_private_readback_ms = not_before_ms;
            }
            needs_readback = false;
            thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
            continue;
        }

        if request.stop_after_first_owned_fill
            && checkpoint.state.phase == GridPhase::Running
            && signed_complete_owned_fill_present_resolved(
                &checkpoint.state.owned_orders,
                &commands,
                &readback.fills,
            )
        {
            // A lifecycle Canary must stop at the first exact private fill even when the next
            // inventory observation would immediately enter low-inventory replenishment. The
            // successor process performs the ordinary signed reconciliation and verification;
            // otherwise a small account can keep refilling and waiting for another fill.
            return Ok(Stage7GridReport {
                exchange: venue.exchange().to_owned(),
                turns,
                phase: checkpoint.state.phase,
                private_generation: generation,
                checkpoint_path,
                stopped: false,
                shadow_only: false,
                private_stream_connected,
                first_owned_fill_observed: true,
            });
        }

        if checkpoint.state.phase == GridPhase::Stopping {
            if cancel_visible_owned_orders(
                &mut commands,
                venue,
                &authority,
                writer.as_ref().ok_or(Stage7GridError::Writer)?,
                &binding,
                &checkpoint.state,
                &readback.orders,
            )? {
                needs_readback = true;
                continue;
            }
            if readback
                .all_order_families_empty()
                .map_err(|_| Stage7GridError::OrderFamily)?
            {
                checkpoint.state.owned_orders.clear();
                if control == HedgedGridControlTarget::Stop {
                    save_checkpoint(&checkpoint_store, &checkpoint)?;
                    return Ok(Stage7GridReport {
                        exchange: venue.exchange().to_owned(),
                        turns,
                        phase: checkpoint.state.phase,
                        private_generation: generation,
                        checkpoint_path,
                        stopped: true,
                        shadow_only: false,
                        private_stream_connected,
                        first_owned_fill_observed: false,
                    });
                }
                // An interrupted stop must not restore its optimistic local order projection.
                // This signed empty view proves the stop has settled, so a requested restart can
                // now transition through the ordinary reset path on a fresh private generation.
                checkpoint.state.resume_after_stop()?;
                if control == HedgedGridControlTarget::Reset {
                    let _ = checkpoint.state.request_reset(GridResetReason::Manual)?;
                    control_store.save(&Stage7GridControl {
                        schema_version: 1,
                        binding: binding.clone(),
                        target: HedgedGridControlTarget::Running,
                    })?;
                }
                save_checkpoint(&checkpoint_store, &checkpoint)?;
                needs_readback = true;
                continue;
            }
        }

        // Signed private readback, risk and rule checks can outlive the public freshness window.
        // Drain and durably commit the frames that arrived during those checks before sampling
        // the mutation book; this refreshes data only and grants no writer or risk authority.
        public_ready = public_market.drive(venue, wall_clock_ms()?)?;

        if !public_ready {
            if checkpoint.state.phase == GridPhase::Running
                && (force_order_health_check || order_health_due(&checkpoint, now_ms))
            {
                // Signed order supervision must continue during a public outage. A complete
                // owned fill remains transitional until public recovery lets the unchanged
                // reducer dispatch its normal rolling transaction; keeping the old watermark
                // forces a fresh signed recheck instead of hiding that transition for 30 minutes.
                let transitioning = signed_complete_owned_fill_present_resolved(
                    &checkpoint.state.owned_orders,
                    &commands,
                    &readback.fills,
                );
                persist_order_health(
                    &request.artifacts_root,
                    &checkpoint_store,
                    &mut checkpoint,
                    &commands,
                    &readback,
                    generation,
                    now_ms,
                    transitioning,
                    &mut force_order_health_check,
                    venue.exchange(),
                )?;
            }
            thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
            continue;
        }

        // Signed REST, risk and rule checks may have blocked this turn. Resample the clock before
        // the mutation book gate so an old BBO cannot remain "fresh" merely because `now_ms`
        // was captured before those calls.
        now_ms = wall_clock_ms()?;
        let (bid, ask) = match venue.best_bid_ask(now_ms) {
            Ok(book) => book,
            Err(error) => {
                warn!(
                    event = "stage7_public_book_failed",
                    exchange = venue.exchange(),
                    reason = %error,
                    "durably captured public book became unusable; retaining private supervision while mutation stays fenced"
                );
                thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
                continue;
            }
        };
        let inventory = inventory(&readback, generation, now_ms, bid, ask, &binding.symbol)?;

        if checkpoint.state.phase == GridPhase::ReplenishingInventory {
            checkpoint
                .state
                .observe_replenishment_inventory(inventory.clone())?;
            if checkpoint
                .state
                .pending_replenishments
                .values()
                .any(|pending| generation <= pending.private_generation)
                || inventory_low(&checkpoint.state, &inventory)
            {
                save_checkpoint(&checkpoint_store, &checkpoint)?;
                thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
                continue;
            }
            let _ = checkpoint.state.settle_pending_replenishments()?;
            save_checkpoint(&checkpoint_store, &checkpoint)?;
        }

        if checkpoint.state.phase == GridPhase::ResettingGrid {
            checkpoint.state.observe_inventory(inventory.clone())?;
            if cancel_visible_owned_orders(
                &mut commands,
                venue,
                &authority,
                writer.as_ref().ok_or(Stage7GridError::Writer)?,
                &binding,
                &checkpoint.state,
                &readback.orders,
            )? {
                save_checkpoint(&checkpoint_store, &checkpoint)?;
                needs_readback = true;
                continue;
            }
            checkpoint.state.reset_orders_settled()?;
            if checkpoint.state.reset_reason == Some(GridResetReason::InventoryLow) {
                checkpoint.state.reconcile_replenishment_round(
                    highest_durable_replenishment_round(&commands, &binding)?,
                )?;
                let GridDecision::Actions(actions) = checkpoint.state.begin_replenishment()? else {
                    return Err(Stage7GridError::Strategy(HedgedGridError::Phase));
                };
                let mutations = actions
                    .into_iter()
                    .filter_map(|action| match action {
                        GridAction::Replenish(value) => Some(value),
                        GridAction::Reset { .. }
                        | GridAction::Place(_)
                        | GridAction::Dispatch(_)
                        | GridAction::ReanchorAtFill { .. } => None,
                    })
                    .map(|replenishment| {
                        let mutation = stage7_market_command(
                            &binding,
                            &replenishment,
                            venue,
                            &inventory,
                            bid,
                            ask,
                        )?;
                        assert_market_notional(&mutation, bid, ask, venue.instrument())?;
                        Ok(mutation)
                    })
                    .collect::<Result<Vec<_>, Stage7GridError>>()?;
                save_checkpoint(&checkpoint_store, &checkpoint)?;
                execute_mutations(
                    &mut commands,
                    venue,
                    &authority,
                    writer.as_ref().ok_or(Stage7GridError::Writer)?,
                    mutations,
                    true,
                )?;
                needs_readback = true;
                continue;
            }
            install_epoch_with_public_refresh(
                &mut checkpoint,
                &mut commands,
                venue,
                &authority,
                writer.as_ref().ok_or(Stage7GridError::Writer)?,
                &binding,
                &inventory,
                bid,
                ask,
                &checkpoint_store,
                |venue| public_market.drive(venue, wall_clock_ms()?),
            )?;
            needs_readback = true;
            continue;
        }

        let phase_before_inventory = checkpoint.state.phase;
        let decision = checkpoint.state.observe_inventory(inventory.clone())?;
        save_checkpoint(&checkpoint_store, &checkpoint)?;
        match decision {
            GridDecision::Actions(actions) => {
                if actions
                    .iter()
                    .all(|action| matches!(action, GridAction::Reset { .. }))
                {
                    if request.stop_after_first_owned_fill
                        && canary_observed_owned_execution(phase_before_inventory, &actions)
                    {
                        // A complete Bitget fill can temporarily lack a client identity on the
                        // account-wide fills surface. A running grid's signed InventoryLow reset
                        // is still an owned execution transition: scope validation has already
                        // excluded foreign orders, and the successor will reconcile it before
                        // validation or cleanup. Stop here so a small-balance Canary cannot
                        // repeatedly replenish while waiting for a richer retired-order record.
                        return Ok(Stage7GridReport {
                            exchange: venue.exchange().to_owned(),
                            turns,
                            phase: checkpoint.state.phase,
                            private_generation: generation,
                            checkpoint_path,
                            stopped: false,
                            shadow_only: false,
                            private_stream_connected,
                            first_owned_fill_observed: true,
                        });
                    }
                    needs_readback = true;
                    continue;
                }
                return Err(Stage7GridError::Strategy(HedgedGridError::Phase));
            }
            GridDecision::Blocked => return Err(Stage7GridError::Unresolved),
            GridDecision::Noop => {}
        }

        if checkpoint.state.phase == GridPhase::Running
            && checkpoint.state.inventory_recovery
                == crate::strategy::hedged_grid::InventoryRecoveryState::Inactive
            && signed_desired_ladder_is_complete(&checkpoint.state, &commands, &binding, &readback)?
        {
            stage7_inventory_recovery_evidence::capture_stage7_settlement(
                &request.artifacts_root,
                &checkpoint,
                &commands,
                readback.orders.len(),
            )?;
        }

        if matches!(
            checkpoint.state.inventory_recovery,
            crate::strategy::hedged_grid::InventoryRecoveryState::ReanchorPending { .. }
        ) {
            // ReanchorPending was fsynced by the fill entrance. Advance only on this later turn,
            // under the resident writer, so either side of the boundary is crash-resumable.
            checkpoint.state.begin_reanchor_rebuild()?;
            save_checkpoint(&checkpoint_store, &checkpoint)?;
            needs_readback = true;
            continue;
        }

        if matches!(
            checkpoint.state.inventory_recovery,
            crate::strategy::hedged_grid::InventoryRecoveryState::Rebuilding { .. }
        ) {
            if !signed_desired_ladder_is_complete(
                &checkpoint.state,
                &commands,
                &binding,
                &readback,
            )? {
                // Accepted mutation receipts are not open-order facts. Keep the persisted
                // rebuilding trigger until one complete signed projection proves the exact
                // desired identity set installed at the original fill anchor.
                needs_readback = true;
                continue;
            }
            match checkpoint.state.complete_reanchor_rebuild() {
                Ok(()) => {
                    save_checkpoint(&checkpoint_store, &checkpoint)?;
                    stage7_inventory_recovery_evidence::capture_stage7_settlement(
                        &request.artifacts_root,
                        &checkpoint,
                        &commands,
                        readback.orders.len(),
                    )?;
                }
                Err(HedgedGridError::Inventory) => {
                    // The new signed inventory lost full closing capacity during rebuild. The
                    // reducer has already returned to Deficient; retain the installed anchor and
                    // let the ordinary recovery lifecycle arm again later.
                    save_checkpoint(&checkpoint_store, &checkpoint)?;
                }
                Err(error) => return Err(error.into()),
            }
        }

        if checkpoint.state.phase == GridPhase::Running {
            let fill_outcome = process_complete_owned_fills(
                &mut checkpoint,
                &mut commands,
                venue,
                &authority,
                writer.as_ref().ok_or(Stage7GridError::Writer)?,
                &binding,
                &readback,
                &checkpoint_store,
            )?;
            if !fill_outcome.wait_for_fresh_book {
                needs_readback |= fill_outcome.private_reconcile_required;
            }
            if fill_outcome.recenter_required {
                info!(
                    event = "stage7_signed_fill_recenter_required",
                    exchange = venue.exchange(),
                    generation,
                    "签名成交替代价已越过新盘口；立即读取下一签名代并重建"
                );
            }
            let dispatched = fill_outcome.mutation_dispatched;

            if !dispatched
                && let Some((expected_orders, observed_orders)) = reconcile_signed_order_loss(
                    &mut checkpoint.state,
                    &commands,
                    &binding,
                    &readback,
                )?
            {
                // A complete signed account view is authoritative for this strategy owner. Once
                // all terminal fills in that view have been consumed, any remaining missing key
                // is unexplained order loss, not a condition that may wait for a periodic health
                // timer. Rebuild from the exact visible owner set before another mutation.
                save_checkpoint(&checkpoint_store, &checkpoint)?;
                info!(
                    event = "stage7_signed_order_loss_rebuild",
                    exchange = venue.exchange(),
                    generation,
                    expected_orders,
                    observed_orders,
                    "完整签名回读发现策略缺单；不等待周期健康检查，立即按账户事实重建"
                );
                needs_readback = true;
                continue;
            }
            if dispatched && request.stop_after_first_owned_fill {
                return Ok(Stage7GridReport {
                    exchange: venue.exchange().to_owned(),
                    turns,
                    phase: checkpoint.state.phase,
                    private_generation: generation,
                    checkpoint_path,
                    stopped: false,
                    shadow_only: false,
                    private_stream_connected,
                    first_owned_fill_observed: true,
                });
            }
            if !needs_readback && advance_bitget_fill_history_window(&mut checkpoint, venue, now_ms)
            {
                // Once a complete signed view has consumed every owned fill and WAL/local
                // transaction is settled, retain only a bounded overlap for crash recovery.
                // Re-fetching the root's entire account-wide history on every WS event makes
                // Bitget fill handling progressively slower without adding authority.
                save_checkpoint(&checkpoint_store, &checkpoint)?;
            }
            if (force_order_health_check || order_health_due(&checkpoint, now_ms))
                && !fill_outcome.private_reconcile_required
            {
                // A signed fill can remove several visible orders before its derived rolling
                // transaction appears in the next signed open-order readback. Do not classify
                // that intentional transition as a broken grid; the next iteration is already
                // fenced to a fresh private reconciliation before another health evaluation.
                persist_order_health(
                    &request.artifacts_root,
                    &checkpoint_store,
                    &mut checkpoint,
                    &commands,
                    &readback,
                    generation,
                    now_ms,
                    dispatched,
                    &mut force_order_health_check,
                    venue.exchange(),
                )?;
            }
        }
        thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
    }

    Ok(Stage7GridReport {
        exchange: venue.exchange().to_owned(),
        turns,
        phase: checkpoint.state.phase,
        private_generation: checkpoint.private_generation,
        checkpoint_path,
        stopped: false,
        shadow_only: request.shadow_only,
        private_stream_connected,
        first_owned_fill_observed: false,
    })
}

pub(super) const fn scheduled_private_readback_allowed(
    public_ready: bool,
    latched_repair_pending: bool,
) -> bool {
    // A public-deferred repair already has a complete signed generation. Periodic and health
    // timers must not turn its 100ms public wait into a full-account REST loop. Explicit Stop,
    // private events and public recovery set `needs_readback` independently and retain priority.
    public_ready || !latched_repair_pending
}

/// Runs the shared reducer against fresh signed facts without taking ownership of the observed
/// binding. This path must remain locally read-only so it can inspect a predecessor's artifacts
/// while that predecessor is still the only live writer.
fn run_stage7_grid_shadow<V: HedgedGridVenue>(
    cfg: &Config,
    request: Stage7GridRequest,
    binding: HedgedGridBinding,
    venue: &mut V,
) -> Result<Stage7GridReport, Stage7GridError> {
    if request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_dir() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let params = release_params(cfg, &binding)?;
    let control_store = ProjectionStore::new(request.artifacts_root.join(CONTROL_FILE));
    let checkpoint_path = request.artifacts_root.join(CHECKPOINT_FILE);
    let checkpoint_store = ProjectionStore::new(&checkpoint_path);
    let checkpoint = load_checkpoint(&checkpoint_store, &binding, &params, false)?;
    let _control = read_control(&control_store, &binding)?;
    // Recovery validation is deliberately retained, but none of these read-only handles may
    // prepare, repair, append, or otherwise alter the predecessor's durable state.
    let commands = CommandJournal::open(request.artifacts_root.join(COMMAND_FILE))?;
    let evidence = open_stage7_private_evidence(&request.artifacts_root, &binding)?;
    let authority = WriterLeaseAuthority::open(
        request.artifacts_root.join(WRITER_FILE),
        stage7_writer_scope(&binding),
    )?;
    let _active_writer = authority.active_session()?;
    let mut public_market = Stage7PublicRuntime::open_read_only(&request.artifacts_root, &binding)?;
    let mut generation = checkpoint
        .private_generation
        .max(evidence.last_generation());
    let mut turns = 0_u64;
    let mut private_stream_connected = false;

    loop {
        if request.max_turns.is_some_and(|limit| turns >= limit) {
            break;
        }
        if let Some(deadline_ms) = request.wall_clock_deadline_ms
            && wall_clock_ms()? >= deadline_ms
        {
            break;
        }
        turns = turns.checked_add(1).ok_or(Stage7GridError::Clock)?;
        let now_ms = wall_clock_ms()?;

        if let Err(error) = venue.connect_private_stream() {
            warn!(
                event = "stage7_shadow_private_stream_unavailable",
                exchange = venue.exchange(),
                reason = %error,
                "只读影子私有流不可用；仅保留签名 readback 预览"
            );
        } else {
            private_stream_connected = true;
            let stream_result = (|| {
                for _ in 0..MAX_PRIVATE_EVENTS_PER_TURN {
                    if venue.next_private_event()?.is_none() {
                        break;
                    }
                }
                Ok::<(), GridVenueError>(())
            })();
            if let Err(error) = stream_result {
                warn!(
                    event = "stage7_shadow_private_stream_reset",
                    exchange = venue.exchange(),
                    reason = %error,
                    "只读影子私有流已围栏；不持久化或重放其事件"
                );
                venue.reset_private_stream();
                private_stream_connected = false;
            }
        }

        let public_ready = public_market.drive(venue, now_ms)?;
        let readback = match venue.readback() {
            Ok(readback) => readback,
            Err(error) if is_transient_readback_error(&error) => {
                warn!(
                    event = "stage7_shadow_private_readback_backoff",
                    exchange = venue.exchange(),
                    reason = %error,
                    "只读影子签名 readback 暂不可用；未写入任何恢复工件"
                );
                thread::sleep(Duration::from_millis(250));
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        require_complete_order_family_readback(&readback)?;
        generation = generation.checked_add(1).ok_or(Stage7GridError::Clock)?;
        if public_ready && let Ok((bid, ask)) = venue.best_bid_ask(now_ms) {
            let observed = inventory(&readback, generation, now_ms, bid, ask, &binding.symbol)?;
            let mut preview = checkpoint.state.clone();
            let decision = preview.observe_inventory(observed)?;
            info!(
                event = "stage7_grid_shadow_strategy_preview",
                exchange = venue.exchange(),
                generation,
                phase = ?preview.phase,
                decision = ?decision,
                observed_orders = readback.orders.len(),
                unresolved_wal = commands.has_unresolved(),
                "共享网格只读影子已用完整签名事实执行 reducer 预览"
            );
        }
        thread::sleep(Duration::from_millis(IDLE_SLEEP_MS));
    }

    Ok(Stage7GridReport {
        exchange: venue.exchange().to_owned(),
        turns,
        phase: checkpoint.state.phase,
        private_generation: generation,
        checkpoint_path,
        stopped: false,
        shadow_only: true,
        private_stream_connected,
        first_owned_fill_observed: false,
    })
}

pub(super) const fn exposure_poll_phase_allows(phase: GridPhase) -> bool {
    matches!(phase, GridPhase::ResettingGrid | GridPhase::Running)
}

#[allow(clippy::too_many_arguments)]
fn poll_live_exposure_if_due<V: HedgedGridVenue>(
    settings: Option<&hedged_grid::ExposureRuntimeSettings>,
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
    control: HedgedGridControlTarget,
    now_ms: u64,
    next_risk_snapshot_ms: &mut u64,
    risk_lane: &mut Stage7RiskLane,
) -> Result<bool, Stage7GridError> {
    let Some(settings) = settings else {
        return Ok(false);
    };
    let lane_allowed = checkpoint.private_generation != 0
        && !commands.has_unresolved()
        && control == HedgedGridControlTarget::Running
        && exposure_poll_phase_allows(checkpoint.state.phase);

    let completed = match risk_lane.poll() {
        Ok(completed) => completed,
        Err(error) => {
            return settle_exposure_poll(
                Err(error),
                settings,
                venue.exchange(),
                next_risk_snapshot_ms,
            );
        }
    };
    if let Some(completed) = completed {
        let next_generation = checkpoint
            .private_generation
            .max(evidence.last_generation())
            .checked_add(1)
            .ok_or(Stage7GridError::Clock)?;
        if !lane_allowed || completed.private_generation != next_generation {
            // Private WS/readback facts advanced while the request-only worker was in flight.
            // Its normalized generation can no longer be appended contiguously, so discard it
            // without publishing evidence or evaluating risk and start a new candidate later.
            *next_risk_snapshot_ms = now_ms.saturating_add(settings.snapshot_interval_ms);
            return Ok(false);
        }
        let result = completed
            .result
            .map_err(Stage7GridError::from)
            .and_then(|readback| {
                stage7_exposure::poll_exposure_take_profit(
                    settings,
                    checkpoint,
                    checkpoint_store,
                    commands,
                    evidence,
                    shadow_evidence,
                    venue,
                    authority,
                    writer,
                    binding,
                    shadow_only,
                    now_ms,
                    Some(readback),
                )
            });
        return settle_exposure_poll(result, settings, venue.exchange(), next_risk_snapshot_ms);
    }

    if risk_lane.pending() {
        return Ok(false);
    }
    if !lane_allowed || commands.has_unresolved() || now_ms < *next_risk_snapshot_ms {
        return Ok(false);
    }
    if checkpoint.pending_exposure_reduction.is_some() {
        let result = stage7_exposure::poll_exposure_take_profit(
            settings,
            checkpoint,
            checkpoint_store,
            commands,
            evidence,
            shadow_evidence,
            venue,
            authority,
            writer,
            binding,
            shadow_only,
            now_ms,
            None,
        );
        return settle_exposure_poll(result, settings, venue.exchange(), next_risk_snapshot_ms);
    }
    let generation = checkpoint
        .private_generation
        .max(evidence.last_generation())
        .checked_add(1)
        .ok_or(Stage7GridError::Clock)?;
    match risk_lane.start(binding.account.clone(), generation) {
        Ok(()) => Ok(false),
        Err(error) => settle_exposure_poll(
            Err(error),
            settings,
            venue.exchange(),
            next_risk_snapshot_ms,
        ),
    }
}

fn settle_exposure_poll(
    result: Result<bool, Stage7GridError>,
    settings: &hedged_grid::ExposureRuntimeSettings,
    exchange: &str,
    next_risk_snapshot_ms: &mut u64,
) -> Result<bool, Stage7GridError> {
    match result {
        Ok(dispatched) => {
            *next_risk_snapshot_ms = wall_clock_ms()?.saturating_add(settings.snapshot_interval_ms);
            Ok(dispatched)
        }
        Err(error) => {
            *next_risk_snapshot_ms = wall_clock_ms()?.saturating_add(settings.snapshot_interval_ms);
            warn!(
                event = "stage7_exposure_snapshot_failed_closed",
                exchange,
                retry_not_before_ms = *next_risk_snapshot_ms,
                reason = %error,
                "风险快照失败关闭；风险通道退避，网格成交补撤保持独立"
            );
            Ok(false)
        }
    }
}

pub(super) fn interrupted_wal_settlement_due(
    control: HedgedGridControlTarget,
    state: &HedgedGridState,
    now_ms: u64,
) -> bool {
    control == HedgedGridControlTarget::Stop || state.blocked_reconciliation_is_due(now_ms)
}

pub(super) fn settle_due_interrupted_wal(
    control: HedgedGridControlTarget,
    state: &HedgedGridState,
    commands: &mut CommandJournal,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
    now_ms: u64,
) -> Result<bool, Stage7GridError> {
    if !interrupted_wal_settlement_due(control, state, now_ms) || !commands.has_unresolved() {
        return Ok(false);
    }
    // Stop or the 30-second BlockedUnknown deadline is already durable and this is a complete
    // signed private view. Settle every concurrently interrupted dispatch before generic
    // recovery; an absent Submitted sibling must not block cancellation or reconstruction.
    stage7_executable_handoff::settle_interrupted_wal_from_signed_readback(
        commands, state, binding, readback,
    )?;
    Ok(true)
}
