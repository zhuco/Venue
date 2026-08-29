use super::*;

pub(super) fn require_complete_order_family_readback(
    readback: &GridVenueReadback,
) -> Result<(), Stage7GridError> {
    readback
        .validate_order_family_readback()
        .map_err(|_| Stage7GridError::OrderFamily)
}

/// The normal grid writer has no command or WAL owner for conditional/Algo rows. Those rows
/// must be signed absent before this runtime can continue to issue regular-family mutations.
pub(super) fn require_no_unmanaged_order_family_rows(
    readback: &GridVenueReadback,
) -> Result<(), Stage7GridError> {
    if readback
        .unmanaged_order_families_are_empty()
        .map_err(|_| Stage7GridError::OrderFamily)?
    {
        Ok(())
    } else {
        Err(Stage7GridError::ForeignOrders)
    }
}

pub(super) fn verify_readback_scope(
    state: &HedgedGridState,
    commands: &CommandJournal,
    readback: &GridVenueReadback,
    binding: &HedgedGridBinding,
) -> Result<(), Stage7GridError> {
    if !readback.hedge_position
        || !stage7_balance_asset_matches_binding(binding, readback.balance.asset.as_str())
    {
        return Err(Stage7GridError::Inventory);
    }
    for order in &readback.orders {
        if validate_signed_exposure_order(order, commands, binding)? {
            continue;
        }
        let _ = validate_signed_checkpoint_order(order, state, commands, binding)?;
    }
    Ok(())
}

/// A submitted/unknown place may become visible before its response is durable. The complete
/// signed open-order surface may settle that same WAL identity, but only after its full physical
/// semantics and exact owner binding have been proved. Prepared or rejected commands are never
/// upgraded from an exchange payload.
pub(super) fn settle_signed_visible_order_receipts(
    commands: &mut CommandJournal,
    state: &HedgedGridState,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
) -> Result<(), Stage7GridError> {
    for order in &readback.orders {
        if !matches!(order.state, OrderState::New | OrderState::PartiallyFilled) {
            return Err(Stage7GridError::Unresolved);
        }
        let FieldState::Known(client_id) = &order.client_order_id else {
            return Err(Stage7GridError::ForeignOrders);
        };
        let client_id = CommandId::new(client_id).map_err(|_| Stage7GridError::ForeignOrders)?;
        if validate_signed_exposure_order(order, commands, binding)? {
            let command_id = commands
                .command_id_by_client_id(&client_id)
                .cloned()
                .ok_or(Stage7GridError::Unresolved)?;
            let state = commands
                .receipt(&command_id)
                .map(|receipt| receipt.state.clone())
                .ok_or(Stage7GridError::Unresolved)?;
            match state {
                CommandState::Accepted { venue_order_id }
                    if accepted_venue_order_id_matches(
                        &venue_order_id,
                        &order.order_id,
                        &client_id,
                        binding,
                    ) => {}
                CommandState::Accepted { .. } => return Err(Stage7GridError::ForeignOrders),
                CommandState::Submitted | CommandState::Unknown { .. } => {
                    commands.transition(
                        &command_id,
                        CommandState::Accepted {
                            venue_order_id: order.order_id.clone(),
                        },
                    )?;
                }
                CommandState::Prepared | CommandState::Rejected { .. } => {
                    return Err(Stage7GridError::Unresolved);
                }
            }
            continue;
        }
        let key = parse_grid_client_order_id(client_id.as_str())?;
        if client_order_id(&key)?.as_str() != client_id.as_str() {
            return Err(Stage7GridError::ForeignOrders);
        }
        let command_id = commands
            .command_id_by_client_id(&client_id)
            .cloned()
            .ok_or(Stage7GridError::Unresolved)?;
        let command = commands
            .place_by_client_id(&client_id)
            .cloned()
            .ok_or(Stage7GridError::Unresolved)?;
        let recovered_intent;
        let intent = match state.owned_orders.get(&key) {
            Some(intent) => intent,
            None if matches!(state.phase, GridPhase::BlockedUnknown | GridPhase::Stopping) => {
                // A failed rolling batch can retire its cancel target optimistically before the
                // venue rejects one child. During reconciliation/Stop, the accepted place WAL is
                // the ownership source; normal Running still requires the checkpoint intent.
                recovered_intent = GridOrderIntent {
                    key: key.clone(),
                    side: command.side,
                    price: command.limit_price,
                    quantity: command.quantity,
                    reduce_only: command.reduce_only,
                };
                recovered_intent.validate()?;
                &recovered_intent
            }
            None => return Err(Stage7GridError::ForeignOrders),
        };
        if intent.key != key {
            return Err(Stage7GridError::Unresolved);
        }
        validate_signed_order_physical_semantics(order, intent, &command, binding, &client_id)?;
        let state = commands
            .receipt(&command_id)
            .map(|receipt| receipt.state.clone())
            .ok_or(Stage7GridError::Unresolved)?;
        match state {
            CommandState::Accepted { venue_order_id }
                if accepted_venue_order_id_matches(
                    &venue_order_id,
                    &order.order_id,
                    &client_id,
                    binding,
                ) => {}
            CommandState::Accepted { .. } => return Err(Stage7GridError::ForeignOrders),
            CommandState::Submitted | CommandState::Unknown { .. } => {
                commands.transition(
                    &command_id,
                    CommandState::Accepted {
                        venue_order_id: order.order_id.clone(),
                    },
                )?;
            }
            CommandState::Prepared | CommandState::Rejected { .. } => {
                return Err(Stage7GridError::Unresolved);
            }
        }
    }
    Ok(())
}

/// Proves that an active signed order is the exact physical projection of both the checkpoint
/// intent and a terminally accepted WAL command. A parseable grid identity is only a lookup key;
/// it is never sufficient ownership evidence by itself.
pub(super) fn validate_signed_checkpoint_order(
    order: &Order,
    state: &HedgedGridState,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
) -> Result<GridOrderKey, Stage7GridError> {
    if !matches!(order.state, OrderState::New | OrderState::PartiallyFilled) {
        return Err(Stage7GridError::Unresolved);
    }
    let FieldState::Known(client_id) = &order.client_order_id else {
        return Err(Stage7GridError::ForeignOrders);
    };
    let key = parse_grid_client_order_id(client_id)?;
    let expected_client_id = client_order_id(&key)?;
    if client_id != expected_client_id.as_str() {
        return Err(Stage7GridError::ForeignOrders);
    }
    let intent = state
        .owned_orders
        .get(&key)
        .ok_or(Stage7GridError::ForeignOrders)?;
    if intent.key != key {
        return Err(Stage7GridError::Unresolved);
    }
    validate_signed_order_against_intent(order, intent, commands, binding)?;
    Ok(key)
}

pub(super) fn validate_signed_exposure_order(
    order: &Order,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
) -> Result<bool, Stage7GridError> {
    let FieldState::Known(client_id) = &order.client_order_id else {
        return Ok(false);
    };
    let client_id = CommandId::new(client_id).map_err(|_| Stage7GridError::ForeignOrders)?;
    let Some(command) = commands.market_reduce_by_client_id(&client_id) else {
        return Ok(false);
    };
    validate_owner_binding(&command.owner, binding).map_err(|_| Stage7GridError::ForeignOrders)?;
    if command.owner.purpose != OrderPurpose::ExposureTakeProfit
        || order.symbol != binding.symbol
        || order.side != command.side
        || order.position_side != FieldState::Known(command.position_side)
        || order.quantity != command.quantity
        || order.limit_price.is_some()
        || !matches!(order.state, OrderState::New | OrderState::PartiallyFilled)
    {
        return Err(Stage7GridError::ForeignOrders);
    }
    Ok(true)
}

pub(super) fn validate_signed_order_against_intent(
    order: &Order,
    intent: &GridOrderIntent,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
) -> Result<(), Stage7GridError> {
    let FieldState::Known(client_id) = &order.client_order_id else {
        return Err(Stage7GridError::ForeignOrders);
    };
    let client_id = CommandId::new(client_id).map_err(|_| Stage7GridError::ForeignOrders)?;
    let command_id = commands
        .command_id_by_client_id(&client_id)
        .ok_or(Stage7GridError::Unresolved)?;
    match commands.receipt(command_id).map(|receipt| &receipt.state) {
        Some(CommandState::Accepted { venue_order_id })
            if accepted_venue_order_id_matches(
                venue_order_id,
                &order.order_id,
                &client_id,
                binding,
            ) => {}
        Some(CommandState::Accepted { .. }) => return Err(Stage7GridError::ForeignOrders),
        _ => return Err(Stage7GridError::Unresolved),
    }
    let command = commands
        .place_by_client_id(&client_id)
        .ok_or(Stage7GridError::Unresolved)?;
    validate_signed_order_physical_semantics(order, intent, command, binding, &client_id)
}

pub(super) fn validate_signed_order_physical_semantics(
    order: &Order,
    intent: &GridOrderIntent,
    command: &crate::domain::OrderCommand,
    binding: &HedgedGridBinding,
    client_id: &CommandId,
) -> Result<(), Stage7GridError> {
    validate_owner_binding(&command.owner, binding).map_err(|_| Stage7GridError::ForeignOrders)?;

    let expected_position_side = match intent.key.position {
        GridPosition::Long => PositionSide::Long,
        GridPosition::Short => PositionSide::Short,
    };
    if order.symbol != binding.symbol
        || command.client_order_id != *client_id
        || command.side != intent.side
        || command.position_side != expected_position_side
        || command.reduce_only != intent.reduce_only
        || command.limit_price != intent.price
        || !checkpoint_quantity_matches(
            command.quantity,
            intent.quantity,
            intent.reduce_only,
            order.state,
            order.filled_quantity,
        )
    {
        return Err(Stage7GridError::Unresolved);
    }
    if order.side != intent.side
        || order.position_side != FieldState::Known(expected_position_side)
        || order.reduce_only != intent.reduce_only
        || order.limit_price != Some(intent.price)
        || !checkpoint_quantity_matches(
            intent.quantity,
            order.quantity,
            intent.reduce_only,
            order.state,
            order.filled_quantity,
        )
    {
        return Err(Stage7GridError::ForeignOrders);
    }
    Ok(())
}

/// Repairs a projection that lags the signed open-order view only when the entire replacement
/// view is bound to terminally accepted commands from this exact strategy owner. This can happen
/// after a process interruption between a venue acknowledgement and checkpoint persistence.
/// Unknown, foreign, or unresolved orders keep the fail-closed scope boundary below.
pub(super) fn reconcile_visible_order_drift(
    state: &mut HedgedGridState,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
) -> Result<bool, Stage7GridError> {
    if state.phase == GridPhase::BlockedUnknown {
        return Ok(false);
    }

    for order in &readback.orders {
        if validate_signed_exposure_order(order, commands, binding)? {
            continue;
        }
        let _ = validate_signed_checkpoint_order(order, state, commands, binding)?;
    }
    Ok(false)
}

/// A venue can acknowledge an exact cancel before its paged open-order view removes the target.
/// Wait for another signed generation only when every checkpoint-retired visible order is still
/// the exact accepted place identity and has its own terminally accepted cancel. This narrow
/// transition cannot hide an external order or turn a local cancel acknowledgement into an order
/// fact.
pub(super) fn signed_readback_contains_settling_owned_cancel(
    state: &HedgedGridState,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
) -> bool {
    let mut settling = false;
    for order in &readback.orders {
        let FieldState::Known(client_id) = &order.client_order_id else {
            return false;
        };
        let Ok(client_id) = CommandId::new(client_id) else {
            return false;
        };
        let Ok(key) = parse_grid_client_order_id(client_id.as_str()) else {
            return false;
        };
        if state.owned_orders.contains_key(&key) {
            continue;
        }
        let Ok(expected_client_id) = client_order_id(&key) else {
            return false;
        };
        if expected_client_id != client_id || !commands.has_accepted_cancel_for(&client_id) {
            return false;
        }
        let Some(command_id) = commands.command_id_by_client_id(&client_id) else {
            return false;
        };
        let Some(CommandState::Accepted { venue_order_id }) =
            commands.receipt(command_id).map(|receipt| &receipt.state)
        else {
            return false;
        };
        let Some(command) = commands.place_by_client_id(&client_id) else {
            return false;
        };
        let intent = GridOrderIntent {
            key,
            side: command.side,
            price: command.limit_price,
            quantity: command.quantity,
            reduce_only: command.reduce_only,
        };
        if !accepted_venue_order_id_matches(venue_order_id, &order.order_id, &client_id, binding)
            || validate_signed_order_physical_semantics(
                order, &intent, command, binding, &client_id,
            )
            .is_err()
        {
            return false;
        }
        settling = true;
    }
    settling
}

/// Rebuilds the live projection only from terminally accepted, binding-scoped commands observed
/// in the current signed open-order view. This is the sole exit from `BlockedUnknown`.
pub(super) fn recovered_owned_orders(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
) -> Result<
    BTreeMap<
        crate::strategy::hedged_grid::GridOrderKey,
        crate::strategy::hedged_grid::GridOrderIntent,
    >,
    Stage7GridError,
> {
    let mut recovered = BTreeMap::new();
    for order in &readback.orders {
        if !matches!(order.state, OrderState::New | OrderState::PartiallyFilled) {
            return Err(Stage7GridError::Unresolved);
        }
        let FieldState::Known(client_id) = &order.client_order_id else {
            return Err(Stage7GridError::ForeignOrders);
        };
        let client_id = CommandId::new(client_id).map_err(|_| Stage7GridError::ForeignOrders)?;
        let key = parse_grid_client_order_id(client_id.as_str())?;
        if client_order_id(&key)?.as_str() != client_id.as_str() {
            return Err(Stage7GridError::ForeignOrders);
        }
        let command_id = commands
            .command_id_by_client_id(&client_id)
            .ok_or(Stage7GridError::Unresolved)?;
        match commands.receipt(command_id).map(|receipt| &receipt.state) {
            Some(CommandState::Accepted { venue_order_id })
                if accepted_venue_order_id_matches(
                    venue_order_id,
                    &order.order_id,
                    &client_id,
                    binding,
                ) => {}
            Some(CommandState::Accepted { .. }) => return Err(Stage7GridError::ForeignOrders),
            _ => return Err(Stage7GridError::Unresolved),
        }
        let command = commands
            .place_by_client_id(&client_id)
            .ok_or(Stage7GridError::Unresolved)?;
        validate_owner_binding(&command.owner, binding)
            .map_err(|_| Stage7GridError::ForeignOrders)?;
        if order.symbol != binding.symbol
            || command.client_order_id != client_id
            || command.side != order.side
            || !matches!(
                order.position_side,
                FieldState::Known(position_side) if position_side == command.position_side
            )
            || command.reduce_only != order.reduce_only
            || !checkpoint_quantity_matches(
                command.quantity,
                order.quantity,
                command.reduce_only,
                order.state,
                order.filled_quantity,
            )
            || order.limit_price != Some(command.limit_price)
        {
            return Err(Stage7GridError::Unresolved);
        }
        let intent = GridOrderIntent {
            key: key.clone(),
            side: command.side,
            price: command.limit_price,
            quantity: order.quantity,
            reduce_only: command.reduce_only,
        };
        intent.validate()?;
        if recovered.insert(key, intent).is_some() {
            return Err(Stage7GridError::Unresolved);
        }
    }
    Ok(recovered)
}

/// One early shared Binance release journaled the complete PAPI acknowledgement instead of its
/// native `orderId`. Accept that immutable WAL value only when both identities inside the JSON
/// exactly match the signed order; every new mutation journals the scalar native id.
pub(super) fn accepted_venue_order_id_matches(
    journaled: &str,
    signed_order_id: &str,
    client_order_id: &CommandId,
    binding: &HedgedGridBinding,
) -> bool {
    if journaled == signed_order_id {
        return true;
    }
    if binding.exchange != "binance" {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(journaled) else {
        return false;
    };
    if value
        .get("clientOrderId")
        .and_then(serde_json::Value::as_str)
        != Some(client_order_id.as_str())
    {
        return false;
    }
    match value.get("orderId") {
        Some(serde_json::Value::String(order_id)) => order_id == signed_order_id,
        Some(serde_json::Value::Number(order_id)) => order_id.to_string() == signed_order_id,
        _ => false,
    }
}

/// Final reanchor completion is gated by the complete signed desired identity set. Physical
/// opening quantity may be normalized by venue rules, so recovery compares the accepted WAL-bound
/// keys after `recovered_owned_orders` has validated every physical field.
pub(super) fn signed_desired_ladder_is_complete(
    state: &HedgedGridState,
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    readback: &GridVenueReadback,
) -> Result<bool, Stage7GridError> {
    let recovered = recovered_owned_orders(commands, binding, readback)?;
    Ok(recovered.len() == state.owned_orders.len()
        && recovered
            .keys()
            .all(|key| state.owned_orders.contains_key(key)))
}

pub(super) fn checkpoint_quantity_matches(
    requested: Decimal,
    observed: Decimal,
    reduce_only: bool,
    state: OrderState,
    filled_quantity: Decimal,
) -> bool {
    observed == requested
        || (reduce_only
            && state == OrderState::PartiallyFilled
            && filled_quantity > Decimal::ZERO
            && observed > Decimal::ZERO
            && observed < requested
            && observed + filled_quantity == requested)
}

pub(super) fn inventory(
    readback: &GridVenueReadback,
    generation: u64,
    now_ms: u64,
    bid: Price,
    ask: Price,
    symbol: &crate::domain::Symbol,
) -> Result<GridInventory, Stage7GridError> {
    let long = position_or_flat(&readback.positions, PositionSide::Long, symbol)?;
    let short = position_or_flat(&readback.positions, PositionSide::Short, symbol)?;
    let fallback = Price::new((bid.value() + ask.value()) / Decimal::TWO)
        .map_err(|_| Stage7GridError::Inventory)?;
    let mark_price = match (long.mark_price, short.mark_price) {
        (Some(long), Some(short)) if long == short => long,
        (Some(mark), None) if short.quantity.is_zero() => mark,
        (None, Some(mark)) if long.quantity.is_zero() => mark,
        (None, None) if long.quantity.is_zero() && short.quantity.is_zero() => fallback,
        _ => return Err(Stage7GridError::Inventory),
    };
    Ok(GridInventory {
        private_generation: generation,
        private_observed_at_ms: now_ms,
        mark_price,
        long_quantity: long.quantity,
        short_quantity: short.quantity,
    })
}

pub(super) fn position_or_flat(
    positions: &[Position],
    side: PositionSide,
    symbol: &crate::domain::Symbol,
) -> Result<Position, Stage7GridError> {
    let mut matches = positions.iter().filter(|position| position.side == side);
    let position = matches.next().cloned().unwrap_or(Position {
        symbol: symbol.clone(),
        side,
        quantity: Decimal::ZERO,
        entry_price: None,
        mark_price: None,
    });
    if matches.next().is_some() {
        return Err(Stage7GridError::Inventory);
    }
    Ok(position)
}

pub(super) fn inventory_low(state: &HedgedGridState, inventory: &GridInventory) -> bool {
    inventory.notional(GridPosition::Long) < state.params.order_notional.value
        || inventory.notional(GridPosition::Short) < state.params.order_notional.value
}
