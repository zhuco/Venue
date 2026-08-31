use super::*;

pub fn run_gate_stage7_canary(
    cfg: &Config,
    request: Stage7CanaryRequest,
) -> Result<Stage7CanaryReport, Stage7GridError> {
    let binding = gate_binding(cfg)?;
    let params = release_params(cfg, &binding)?;
    let venue = GateGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_canary(request, binding, params, venue, "gpc")
}

pub fn run_binance_stage7_canary(
    cfg: &Config,
    request: Stage7CanaryRequest,
) -> Result<Stage7CanaryReport, Stage7GridError> {
    let binding = binance_binding(cfg)?;
    let params = release_params(cfg, &binding)?;
    let venue = BinanceGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_canary(request, binding, params, venue, "bnc")
}

fn run_stage7_canary<V: Stage7CanaryVenue>(
    request: Stage7CanaryRequest,
    binding: HedgedGridBinding,
    params: HedgedGridParams,
    mut venue: V,
    prefix: &str,
) -> Result<Stage7CanaryReport, Stage7GridError> {
    if !request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let writer_scope = stage7_writer_scope(&binding);
    let _canonical_root_guard = acquire_stage7_writer_root(&writer_scope, &request.artifacts_root)?;
    venue.verify_current_instrument_rules()?;
    let mut public_market = Stage7PublicRuntime::open(&request.artifacts_root, &binding)?;
    let mut commands = CommandJournal::open(request.artifacts_root.join(COMMAND_FILE))?;
    if commands.has_unresolved() {
        return Err(Stage7GridError::Unresolved);
    }
    let mut evidence = open_stage7_private_evidence(&request.artifacts_root, &binding)?;
    let authority =
        WriterLeaseAuthority::open(request.artifacts_root.join(WRITER_FILE), writer_scope)?;
    if authority.active_session()?.is_some() {
        return Err(Stage7GridError::Writer);
    }
    let mut generation = evidence.last_generation();
    let (readback, inventory, bid, ask) = canary_readback(
        &mut venue,
        &mut public_market,
        &mut evidence,
        &mut generation,
        &binding,
    )?;
    canary_preflight(&readback, &inventory)?;
    venue.connect_private_stream()?;
    let now_ms = wall_clock_ms()?;
    let writer = authority.register_initial(now_ms, generation)?;
    let suffix = now_ms / 1_000;
    let result = (|| {
        let passive_price =
            Price::new(bid.value() - venue.instrument().price_tick.value() * Decimal::from(10_u8))
                .map_err(|_| Stage7GridError::Notional)?;
        let quantity = canary_quantity(&venue, bid, ask, passive_price)?;
        let passive = crate::domain::OrderCommand {
            time_in_force: Default::default(),
            command_id: command_id(format!("{prefix}_{suffix}_post_cmd"))?,
            client_order_id: command_id(format!("{prefix}_{suffix}_post"))?,
            owner: canary_owner(&binding, OrderPurpose::Entry),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity,
            limit_price: passive_price,
            reduce_only: false,
        };
        assert_order_notional(passive.quantity, passive.limit_price, venue.instrument())?;
        execute_mutations(
            &mut commands,
            &mut venue,
            &authority,
            &writer,
            vec![Stage7Mutation::Place(passive.clone())],
            true,
        )?;
        let (after_place, _, _, _) = canary_readback(
            &mut venue,
            &mut public_market,
            &mut evidence,
            &mut generation,
            &binding,
        )?;
        let visible = after_place.orders.iter().any(|order| {
        matches!(&order.client_order_id, FieldState::Known(value) if value == passive.client_order_id.as_str())
            && order.position_side == FieldState::Known(PositionSide::Long)
            && order.limit_price == Some(passive.limit_price)
            && !order.reduce_only
    });
        if !visible {
            return Err(Stage7GridError::Canary);
        }
        venue.verify_post_only_order(passive.client_order_id.as_str())?;
        let cancel = CancelCommand {
            command_id: command_id(format!("{prefix}_{suffix}_cancel"))?,
            owner: canary_owner(&binding, OrderPurpose::Entry),
            target_client_order_id: passive.client_order_id.clone(),
        };
        execute_mutations(
            &mut commands,
            &mut venue,
            &authority,
            &writer,
            vec![Stage7Mutation::Cancel(cancel)],
            true,
        )?;
        let (after_cancel, after_cancel_inventory, mut bid, mut ask) = canary_readback(
            &mut venue,
            &mut public_market,
            &mut evidence,
            &mut generation,
            &binding,
        )?;
        canary_preflight(&after_cancel, &after_cancel_inventory)?;

        for (position_side, side, name) in [
            (PositionSide::Long, OrderSide::Buy, "long"),
            (PositionSide::Short, OrderSide::Sell, "short"),
        ] {
            let executable_price = match side {
                OrderSide::Buy => ask,
                OrderSide::Sell => bid,
            };
            let quantity = canary_quantity(&venue, bid, ask, executable_price)?;
            let market = MarketOrderCommand {
                command_id: command_id(format!("{prefix}_{suffix}_{name}_m_cmd"))?,
                client_order_id: command_id(format!("{prefix}_{suffix}_{name}_m"))?,
                owner: canary_owner(&binding, OrderPurpose::Entry),
                position_side,
                side,
                quantity,
                reduce_only: false,
            };
            assert_single_market_notional(
                &Stage7Mutation::Market(market.clone()),
                bid,
                ask,
                venue.instrument(),
            )?;
            execute_mutations(
                &mut commands,
                &mut venue,
                &authority,
                &writer,
                vec![Stage7Mutation::Market(market)],
                true,
            )?;
            let (_, held_inventory, _, _) = wait_for_canary_position(
                &mut venue,
                &mut public_market,
                &mut evidence,
                &mut generation,
                &binding,
                position_side,
                true,
            )?;
            let position_quantity = match position_side {
                PositionSide::Long => held_inventory.long_quantity,
                PositionSide::Short => held_inventory.short_quantity,
                PositionSide::Net => return Err(Stage7GridError::Canary),
            };
            stage7_canary_limit::verify_reduce_only_post_only(
                &mut commands,
                &mut venue,
                &mut public_market,
                &authority,
                &writer,
                &mut evidence,
                &mut generation,
                &binding,
                prefix,
                suffix,
                position_side,
                position_quantity,
                bid,
                ask,
            )?;
            let reduce = canary_reduce_command(
                &binding,
                prefix,
                suffix,
                name,
                position_side,
                position_quantity,
                generation,
            )?;
            reduce_canary_market(&mut commands, &mut venue, &authority, &writer, reduce)?;
            let (flat_readback, flat_inventory, fresh_bid, fresh_ask) = wait_for_canary_position(
                &mut venue,
                &mut public_market,
                &mut evidence,
                &mut generation,
                &binding,
                position_side,
                false,
            )?;
            canary_preflight(&flat_readback, &flat_inventory)?;
            bid = fresh_bid;
            ask = fresh_ask;
        }

        if commands.has_unresolved() {
            return Err(Stage7GridError::Unresolved);
        }
        let capability_valid_until_ms =
            wall_clock_ms()?.saturating_add(CANARY_CAPABILITY_VALIDITY_MS);
        append_stage7_canary_capabilities(
            &venue.capability_binding(),
            &binding,
            &params,
            &request.artifacts_root,
            generation,
            capability_valid_until_ms,
        )?;
        authority.retire_flat(&FlatReceipt {
            receipt_id: format!("{}_stage7_flat_{generation}", binding.exchange),
            predecessor: writer.clone(),
            scope: WriterScope {
                exchange: binding.exchange.clone(),
                account: binding.account.clone(),
                symbol: binding.symbol.clone(),
                owner_scope: binding.owner_scope.clone(),
            },
            readback_generation: generation,
            summary_sha256: sha256_hex(format!(
                "{}_stage7_flat:{}:{generation}",
                binding.exchange, binding.owner_scope
            )),
        })?;
        Ok(Stage7CanaryReport {
            exchange: binding.exchange.clone(),
            symbol: binding.symbol.to_string(),
            private_generation: generation,
            capability_valid_until_ms,
        })
    })();
    match result {
        Ok(report) => Ok(report),
        Err(error) => {
            warn!(
                event = "stage7_canary_failure",
                exchange = %binding.exchange,
                reason = %error,
                "Stage7 canary failed; starting signed-private cleanup"
            );
            stage7_canary_safety::fail_after_canary_error(
                error,
                &mut commands,
                &mut venue,
                &authority,
                &writer,
                &mut evidence,
                &mut generation,
                &binding,
                prefix,
                suffix,
            )
        }
    }
}

pub fn run_bitget_stage7_canary(
    cfg: &Config,
    request: Stage7CanaryRequest,
) -> Result<Stage7CanaryReport, Stage7GridError> {
    if !request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let binding = bitget_binding(cfg)?;
    let params = release_params(cfg, &binding)?;
    let writer_scope = stage7_writer_scope(&binding);
    let _canonical_root_guard = acquire_stage7_writer_root(&writer_scope, &request.artifacts_root)?;
    let mut venue = BitgetGridVenue::production(binding.symbol.clone(), 1)?;
    venue.verify_current_instrument_rules()?;
    let mut public_market = Stage7PublicRuntime::open(&request.artifacts_root, &binding)?;
    let mut commands = CommandJournal::open(request.artifacts_root.join(COMMAND_FILE))?;
    if commands.has_unresolved() {
        return Err(Stage7GridError::Unresolved);
    }
    let mut evidence = open_stage7_private_evidence(&request.artifacts_root, &binding)?;
    let authority =
        WriterLeaseAuthority::open(request.artifacts_root.join(WRITER_FILE), writer_scope)?;
    if authority.active_session()?.is_some() {
        return Err(Stage7GridError::Writer);
    }
    let mut generation = evidence.last_generation();
    let (readback, inventory, bid, ask) = canary_readback(
        &mut venue,
        &mut public_market,
        &mut evidence,
        &mut generation,
        &binding,
    )?;
    canary_preflight(&readback, &inventory)?;
    venue.connect_private_stream()?;
    let now_ms = wall_clock_ms()?;
    let writer = authority.register_initial(now_ms, generation)?;
    let prefix = "bgc";
    let suffix = now_ms / 1_000;
    let result = (|| {
        let passive_price =
            Price::new(bid.value() - venue.instrument().price_tick.value() * Decimal::from(10_u8))
                .map_err(|_| Stage7GridError::Notional)?;
        let quantity = canary_quantity(&venue, bid, ask, passive_price)?;
        let passive = crate::domain::OrderCommand {
            time_in_force: Default::default(),
            command_id: command_id(format!("bgc_{suffix}_post_cmd"))?,
            client_order_id: command_id(format!("bgc_{suffix}_post"))?,
            owner: canary_owner(&binding, OrderPurpose::Entry),
            side: OrderSide::Buy,
            position_side: PositionSide::Long,
            quantity,
            limit_price: passive_price,
            reduce_only: false,
        };
        assert_order_notional(passive.quantity, passive.limit_price, venue.instrument())?;
        execute_mutations(
            &mut commands,
            &mut venue,
            &authority,
            &writer,
            vec![Stage7Mutation::Place(passive.clone())],
            true,
        )?;
        let (after_place, _, _, _) = canary_readback(
            &mut venue,
            &mut public_market,
            &mut evidence,
            &mut generation,
            &binding,
        )?;
        let visible = after_place.orders.iter().any(|order| {
        matches!(&order.client_order_id, FieldState::Known(value) if value == passive.client_order_id.as_str())
            && order.position_side == FieldState::Known(PositionSide::Long)
            && order.limit_price == Some(passive.limit_price) && !order.reduce_only
    });
        if !visible {
            return Err(Stage7GridError::Canary);
        }
        venue.verify_post_only_order(passive.client_order_id.as_str())?;
        execute_mutations(
            &mut commands,
            &mut venue,
            &authority,
            &writer,
            vec![Stage7Mutation::Cancel(CancelCommand {
                command_id: command_id(format!("bgc_{suffix}_cancel"))?,
                owner: canary_owner(&binding, OrderPurpose::Entry),
                target_client_order_id: passive.client_order_id.clone(),
            })],
            true,
        )?;
        let (after_cancel, after_cancel_inventory, mut bid, mut ask) = canary_readback(
            &mut venue,
            &mut public_market,
            &mut evidence,
            &mut generation,
            &binding,
        )?;
        canary_preflight(&after_cancel, &after_cancel_inventory)?;
        for (position_side, side, name) in [
            (PositionSide::Long, OrderSide::Buy, "long"),
            (PositionSide::Short, OrderSide::Sell, "short"),
        ] {
            let executable_price = match side {
                OrderSide::Buy => ask,
                OrderSide::Sell => bid,
            };
            let quantity = canary_quantity(&venue, bid, ask, executable_price)?;
            let market = MarketOrderCommand {
                command_id: command_id(format!("bgc_{suffix}_{name}_m_cmd"))?,
                client_order_id: command_id(format!("bgc_{suffix}_{name}_m"))?,
                owner: canary_owner(&binding, OrderPurpose::Entry),
                position_side,
                side,
                quantity,
                reduce_only: false,
            };
            assert_single_market_notional(
                &Stage7Mutation::Market(market.clone()),
                bid,
                ask,
                venue.instrument(),
            )?;
            execute_mutations(
                &mut commands,
                &mut venue,
                &authority,
                &writer,
                vec![Stage7Mutation::Market(market)],
                true,
            )?;
            let (_, held_inventory, _, _) = wait_for_canary_position(
                &mut venue,
                &mut public_market,
                &mut evidence,
                &mut generation,
                &binding,
                position_side,
                true,
            )?;
            let position_quantity = match position_side {
                PositionSide::Long => held_inventory.long_quantity,
                PositionSide::Short => held_inventory.short_quantity,
                PositionSide::Net => return Err(Stage7GridError::Canary),
            };
            stage7_canary_limit::verify_reduce_only_post_only(
                &mut commands,
                &mut venue,
                &mut public_market,
                &authority,
                &writer,
                &mut evidence,
                &mut generation,
                &binding,
                prefix,
                suffix,
                position_side,
                position_quantity,
                bid,
                ask,
            )?;
            let reduce = canary_reduce_command(
                &binding,
                prefix,
                suffix,
                name,
                position_side,
                position_quantity,
                generation,
            )?;
            reduce_canary_market(&mut commands, &mut venue, &authority, &writer, reduce)?;
            let (flat_readback, flat_inventory, fresh_bid, fresh_ask) = wait_for_canary_position(
                &mut venue,
                &mut public_market,
                &mut evidence,
                &mut generation,
                &binding,
                position_side,
                false,
            )?;
            canary_preflight(&flat_readback, &flat_inventory)?;
            bid = fresh_bid;
            ask = fresh_ask;
        }
        if commands.has_unresolved() {
            return Err(Stage7GridError::Unresolved);
        }
        let capability_valid_until_ms =
            wall_clock_ms()?.saturating_add(CANARY_CAPABILITY_VALIDITY_MS);
        append_stage7_canary_capabilities(
            &venue.capability_binding(),
            &binding,
            &params,
            &request.artifacts_root,
            generation,
            capability_valid_until_ms,
        )?;
        authority.retire_flat(&FlatReceipt {
            receipt_id: format!("bitget_stage7_flat_{generation}"),
            predecessor: writer.clone(),
            scope: WriterScope {
                exchange: binding.exchange.clone(),
                account: binding.account.clone(),
                symbol: binding.symbol.clone(),
                owner_scope: binding.owner_scope.clone(),
            },
            readback_generation: generation,
            summary_sha256: sha256_hex(format!(
                "bitget_stage7_flat:{}:{generation}",
                binding.owner_scope
            )),
        })?;
        Ok(Stage7CanaryReport {
            exchange: "bitget".to_owned(),
            symbol: binding.symbol.to_string(),
            private_generation: generation,
            capability_valid_until_ms,
        })
    })();
    match result {
        Ok(report) => Ok(report),
        Err(error) => {
            warn!(
                event = "stage7_bitget_canary_failure",
                reason = %error,
                "Bitget canary failed; starting signed-private cleanup"
            );
            stage7_canary_safety::fail_after_canary_error(
                error,
                &mut commands,
                &mut venue,
                &authority,
                &writer,
                &mut evidence,
                &mut generation,
                &binding,
                prefix,
                suffix,
            )
        }
    }
}

/// Resumes only the safety half of an interrupted canary. It never starts a new probe or grid;
/// it may cancel journal-owned leftovers and reduce the signed-private hedge inventory to flat.
#[allow(clippy::too_many_arguments)]
fn canary_reduce_command(
    binding: &HedgedGridBinding,
    prefix: &str,
    suffix: u64,
    name: &str,
    position_side: PositionSide,
    quantity: Decimal,
    position_generation: u64,
) -> Result<crate::domain::MarketReduceCommand, Stage7GridError> {
    let command = crate::domain::MarketReduceCommand {
        command_id: command_id(format!("{prefix}_{suffix}_{name}_r_cmd"))?,
        client_order_id: command_id(format!("{prefix}_{suffix}_{name}_r"))?,
        owner: canary_owner(binding, OrderPurpose::ExposureTakeProfit),
        side: match position_side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
            PositionSide::Net => return Err(Stage7GridError::Canary),
        },
        position_side,
        quantity,
        risk_episode_id: command_id(format!("{prefix}_{suffix}_{name}_r_episode"))?,
        position_generation,
    };
    command.validate().map_err(|_| Stage7GridError::Canary)?;
    Ok(command)
}

pub fn run_gate_stage7_canary_recovery(
    cfg: &Config,
    request: Stage7CanaryRequest,
) -> Result<Stage7CanaryRecoveryReport, Stage7GridError> {
    let binding = gate_binding(cfg)?;
    let _ = release_params(cfg, &binding)?;
    let mut venue = GateGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_canary_recovery(request, binding, &mut venue, "gpc")
}

pub fn run_binance_stage7_canary_recovery(
    cfg: &Config,
    request: Stage7CanaryRequest,
) -> Result<Stage7CanaryRecoveryReport, Stage7GridError> {
    let binding = binance_binding(cfg)?;
    let _ = release_params(cfg, &binding)?;
    let mut venue = BinanceGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_canary_recovery(request, binding, &mut venue, "bnc")
}

/// Resumes only the safety half of an interrupted canary. It never starts a new probe or grid;
/// it may cancel journal-owned leftovers and reduce the signed-private hedge inventory to flat.
pub fn run_bitget_stage7_canary_recovery(
    cfg: &Config,
    request: Stage7CanaryRequest,
) -> Result<Stage7CanaryRecoveryReport, Stage7GridError> {
    let binding = bitget_binding(cfg)?;
    let _ = release_params(cfg, &binding)?;
    let mut venue = BitgetGridVenue::production(binding.symbol.clone(), 1)?;
    run_stage7_canary_recovery(request, binding, &mut venue, "bgc")
}

pub(super) fn run_stage7_canary_recovery<V: Stage7CanaryVenue>(
    request: Stage7CanaryRequest,
    binding: HedgedGridBinding,
    venue: &mut V,
    prefix: &str,
) -> Result<Stage7CanaryRecoveryReport, Stage7GridError> {
    if !request.confirm_mainnet_grid_mutations {
        return Err(Stage7GridError::Confirmation);
    }
    if !request.artifacts_root.is_absolute() {
        return Err(Stage7GridError::ArtifactsRoot);
    }
    let writer_scope = stage7_writer_scope(&binding);
    let _canonical_root_guard = acquire_stage7_writer_root(&writer_scope, &request.artifacts_root)?;
    venue.set_fill_history_start_ms(recovery_fill_history_start(
        &request.artifacts_root,
        &binding,
    )?);
    let mut commands = CommandJournal::open(request.artifacts_root.join(COMMAND_FILE))?;
    let mut evidence = open_stage7_private_evidence(&request.artifacts_root, &binding)?;
    let authority =
        WriterLeaseAuthority::open(request.artifacts_root.join(WRITER_FILE), writer_scope)?;
    let writer = authority.active_session()?.ok_or(Stage7GridError::Writer)?;
    let mut generation = evidence.last_generation();
    let suffix = wall_clock_ms()? / 1_000;
    stage7_canary_safety::recover_interrupted_canary(
        &mut commands,
        venue,
        &authority,
        &writer,
        &mut evidence,
        &mut generation,
        &binding,
        prefix,
        suffix,
    )?;
    Ok(Stage7CanaryRecoveryReport {
        exchange: binding.exchange,
        symbol: binding.symbol.to_string(),
        private_generation: generation,
    })
}

fn recovery_fill_history_start(
    artifacts_root: &Path,
    binding: &HedgedGridBinding,
) -> Result<u64, Stage7GridError> {
    let checkpoint = ProjectionStore::new(artifacts_root.join(CHECKPOINT_FILE))
        .load::<Stage7GridCheckpoint>()?;
    match checkpoint {
        Some(checkpoint)
            if checkpoint.schema_version == 1
                && checkpoint.binding == *binding
                && checkpoint.state.binding == *binding
                && checkpoint.fill_history_start_ms != 0 =>
        {
            Ok(checkpoint.fill_history_start_ms)
        }
        Some(checkpoint)
            if checkpoint.schema_version == 1
                && checkpoint.binding == *binding
                && checkpoint.state.binding == *binding =>
        {
            // The normal grid entry persists this compatibility anchor before mutation. An
            // interrupted legacy canary has no prior Bitget Live admission, so its recovery is
            // allowed to begin a fresh account-history fence.
            wall_clock_ms()
        }
        Some(_) => Err(Stage7GridError::Checkpoint),
        None => wall_clock_ms(),
    }
}
