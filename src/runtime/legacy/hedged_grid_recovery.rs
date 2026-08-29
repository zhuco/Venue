use super::*;

pub(in crate::runtime) fn active_writer(
    authority: &WriterLeaseAuthority,
    previous: Option<WriterSession>,
    now_ms: u64,
    private_readback_cursor: u64,
    snapshot: Option<&PrivateFactsSnapshot>,
    state: &HedgedGridState,
) -> Result<WriterSession, HedgedGridLiveError> {
    match previous.or(authority.active_session()?) {
        Some(session) => match authority.renew(&session, now_ms) {
            Ok(renewed) => Ok(renewed),
            Err(WriterLeaseError::Expired)
                if snapshot.is_some_and(|snapshot| {
                    snapshot.orders.is_empty()
                        && snapshot
                            .positions
                            .iter()
                            .all(|position| position.quantity.is_zero())
                }) =>
            {
                let receipt = FlatReceipt {
                    receipt_id: format!("grid_flat_recovery_{private_readback_cursor}"),
                    predecessor: session.clone(),
                    scope: session.scope.clone(),
                    readback_generation: private_readback_cursor,
                    summary_sha256: sha256_hex(
                        format!(
                            "hedged_grid_flat:{}:{}:{}",
                            session.scope.symbol, private_readback_cursor, session.generation
                        )
                        .as_bytes(),
                    ),
                };
                authority.retire_flat(&receipt)?;
                Ok(authority.register_initial(now_ms, private_readback_cursor)?)
            }
            Err(WriterLeaseError::Expired)
                if snapshot.is_some_and(|snapshot| {
                    snapshot_contains_only_owned_orders(state, snapshot)
                }) =>
            {
                Ok(authority.recover_same_scope_after_readback(
                    &session,
                    private_readback_cursor,
                    now_ms,
                )?)
            }
            Err(error) => Err(error.into()),
        },
        None => Ok(authority.register_initial(now_ms, private_readback_cursor)?),
    }
}

pub(in crate::runtime) fn recover_absent_grid_unknowns(
    commands: &mut CommandJournal,
    transport: &BinancePrivateFactsTransport,
    binding: &HedgedGridBinding,
    _snapshot: &PrivateFactsSnapshot,
) -> Result<(), HedgedGridLiveError> {
    for command_id in commands.unresolved_command_ids() {
        let receipt = commands
            .receipt(&command_id)
            .cloned()
            .ok_or(HedgedGridLiveError::Unresolved)?;
        if !matches!(receipt.state, CommandState::Unknown { .. }) {
            return Err(HedgedGridLiveError::Unresolved);
        }
        let state = if let Some(command) = commands.place(&command_id).cloned() {
            if !owner_matches_binding(&command.owner, binding) {
                return Err(HedgedGridLiveError::Unresolved);
            }
            let mut found_order_id = None;
            for _ in 0..3 {
                match transport
                    .order_by_client_id(&binding.symbol, command.client_order_id.as_str())
                {
                    Ok(payload) => {
                        let order = binance_private::parse_order(&payload, &binding.symbol)?;
                        if !matches!(
                            &order.client_order_id,
                            FieldState::Known(value) if value == command.client_order_id.as_str()
                        ) {
                            return Err(HedgedGridLiveError::Unresolved);
                        }
                        found_order_id = Some(order.order_id);
                        break;
                    }
                    Err(PrivateFactsWorkerError::Private(PrivateError::Rejected {
                        api_code: Some(-2013),
                        ..
                    })) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            found_order_id.map_or_else(
                || CommandState::Rejected {
                    reason: "three_signed_grid_queries_proved_order_absent".to_owned(),
                },
                |venue_order_id| CommandState::Accepted { venue_order_id },
            )
        } else if let Some(command) = commands.market_reduce(&command_id).cloned() {
            if !owner_matches_binding(&command.owner, binding) {
                return Err(HedgedGridLiveError::Unresolved);
            }
            let mut found_order_id = None;
            for _ in 0..3 {
                match transport
                    .order_by_client_id(&binding.symbol, command.client_order_id.as_str())
                {
                    Ok(payload) => {
                        let order = binance_private::parse_order(&payload, &binding.symbol)?;
                        if !matches!(
                            &order.client_order_id,
                            FieldState::Known(value) if value == command.client_order_id.as_str()
                        ) {
                            return Err(HedgedGridLiveError::Unresolved);
                        }
                        found_order_id = Some(order.order_id);
                        break;
                    }
                    Err(PrivateFactsWorkerError::Private(PrivateError::Rejected {
                        api_code: Some(-2013),
                        ..
                    })) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            exposure_order_resolution(found_order_id)
        } else if let Some(cancel) = commands.cancel(&command_id).cloned() {
            if !owner_matches_binding(&cancel.owner, binding) {
                return Err(HedgedGridLiveError::Unresolved);
            }
            let mut absent = 0_u8;
            let mut resolution = None;
            for _ in 0..3 {
                match transport
                    .order_by_client_id(&binding.symbol, cancel.target_client_order_id.as_str())
                {
                    Ok(payload) => {
                        let order = binance_private::parse_order(&payload, &binding.symbol)?;
                        resolution = Some(match order.state {
                            OrderState::New | OrderState::PartiallyFilled => {
                                CommandState::Rejected {
                                    reason: "signed_readback_proved_cancel_not_applied".to_owned(),
                                }
                            }
                            OrderState::Filled
                            | OrderState::Cancelled
                            | OrderState::Expired
                            | OrderState::Rejected => CommandState::Accepted {
                                venue_order_id: order.order_id,
                            },
                            OrderState::Unknown => return Err(HedgedGridLiveError::Unresolved),
                        });
                        break;
                    }
                    Err(PrivateFactsWorkerError::Private(PrivateError::Rejected {
                        api_code: Some(-2013),
                        ..
                    })) => absent = absent.saturating_add(1),
                    Err(error) => return Err(error.into()),
                }
            }
            resolution.unwrap_or_else(|| {
                if absent == 3 {
                    CommandState::Accepted {
                        venue_order_id: format!(
                            "absent:{}",
                            cancel.target_client_order_id.as_str()
                        ),
                    }
                } else {
                    CommandState::Unknown {
                        reason: "grid_cancel_readback_incomplete".to_owned(),
                    }
                }
            })
        } else {
            return Err(HedgedGridLiveError::Unresolved);
        };
        if matches!(state, CommandState::Unknown { .. }) {
            return Err(HedgedGridLiveError::Unresolved);
        }
        let outcome = command_state_name(&state);
        let reason = match &state {
            CommandState::Rejected { reason } | CommandState::Unknown { reason } => reason.clone(),
            CommandState::Prepared | CommandState::Submitted | CommandState::Accepted { .. } => {
                String::new()
            }
        };
        commands.transition(&command_id, state)?;
        info!(
            event = "grid_mutation_readback",
            command_id = command_id.as_str(),
            outcome,
            reason = %reason,
            "不确定请求完成签名回查"
        );
    }
    Ok(())
}

fn exposure_order_resolution(venue_order_id: Option<String>) -> CommandState {
    venue_order_id.map_or_else(
        || CommandState::Unknown {
            reason: "exposure_order_absent_requires_exact_fill_reconciliation".to_owned(),
        },
        |venue_order_id| CommandState::Accepted { venue_order_id },
    )
}

#[cfg(test)]
mod exposure_recovery_tests {
    use super::*;

    #[test]
    fn absent_exposure_order_never_guesses_rejection() {
        assert!(matches!(
            exposure_order_resolution(None),
            CommandState::Unknown { reason }
                if reason == "exposure_order_absent_requires_exact_fill_reconciliation"
        ));
        assert_eq!(
            exposure_order_resolution(Some("venue-1".to_owned())),
            CommandState::Accepted {
                venue_order_id: "venue-1".to_owned()
            }
        );
    }
}

pub(in crate::runtime) fn recovered_owned_orders(
    commands: &CommandJournal,
    binding: &HedgedGridBinding,
    snapshot: &PrivateFactsSnapshot,
) -> Result<BTreeMap<GridOrderKey, GridOrderIntent>, HedgedGridLiveError> {
    let mut recovered = BTreeMap::new();
    for order in &snapshot.orders {
        if !matches!(order.state, OrderState::New | OrderState::PartiallyFilled) {
            return Err(HedgedGridLiveError::Unresolved);
        }
        let FieldState::Known(client_id) = &order.client_order_id else {
            return Err(HedgedGridLiveError::Unresolved);
        };
        let client_id = CommandId::new(client_id).map_err(|_| HedgedGridLiveError::Identifier)?;
        let command_id = commands
            .command_id_by_client_id(&client_id)
            .ok_or(HedgedGridLiveError::Unresolved)?;
        if !matches!(
            commands.receipt(command_id).map(|receipt| &receipt.state),
            Some(CommandState::Accepted { .. })
        ) {
            return Err(HedgedGridLiveError::Unresolved);
        }
        let command = commands
            .place_by_client_id(&client_id)
            .ok_or(HedgedGridLiveError::Unresolved)?;
        if !owner_matches_binding(&command.owner, binding)
            || command.side != order.side
            || !matches!(
                order.position_side,
                FieldState::Known(position_side) if position_side == command.position_side
            )
            || !recovered_quantity_matches(command.quantity, order.quantity, command.reduce_only)
            || order.limit_price != Some(command.limit_price)
        {
            return Err(HedgedGridLiveError::Unresolved);
        }
        let key = parse_grid_client_order_id(client_id.as_str())?;
        let intent = GridOrderIntent {
            key: key.clone(),
            side: command.side,
            price: command.limit_price,
            quantity: order.quantity,
            reduce_only: command.reduce_only,
        };
        intent.validate()?;
        if recovered.insert(key, intent).is_some() {
            return Err(HedgedGridLiveError::Unresolved);
        }
    }
    Ok(recovered)
}

pub(in crate::runtime) fn recovered_quantity_matches(
    requested: Decimal,
    observed: Decimal,
    reduce_only: bool,
) -> bool {
    observed == requested || (reduce_only && observed > Decimal::ZERO && observed < requested)
}

pub(in crate::runtime) fn owner_matches_binding(
    owner: &OrderOwner,
    binding: &HedgedGridBinding,
) -> bool {
    owner.strategy_instance_id == binding.strategy_instance_id
        && owner.run_id == binding.run_id
        && owner.exchange == binding.exchange
        && owner.account == binding.account
        && owner.symbol == binding.symbol
}

pub(in crate::runtime) fn parse_grid_client_order_id(
    value: &str,
) -> Result<GridOrderKey, HedgedGridLiveError> {
    let parts = value.split('_').collect::<Vec<_>>();
    if parts.len() != 5 || parts[0] != "hgo" {
        return Err(HedgedGridLiveError::Identifier);
    }
    let epoch = parts[1]
        .strip_prefix('e')
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(HedgedGridLiveError::Identifier)?;
    let position = match parts[2] {
        "long" => GridPosition::Long,
        "short" => GridPosition::Short,
        _ => return Err(HedgedGridLiveError::Identifier),
    };
    let role = match parts[3] {
        "open" => GridOrderRole::Open,
        "close" => GridOrderRole::Close,
        _ => return Err(HedgedGridLiveError::Identifier),
    };
    let level = parts[4]
        .strip_prefix('l')
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(HedgedGridLiveError::Identifier)?;
    let key = GridOrderKey {
        epoch,
        position,
        role,
        level,
    };
    key.validate()?;
    Ok(key)
}

pub(in crate::runtime) fn snapshot_contains_only_owned_orders(
    state: &HedgedGridState,
    snapshot: &PrivateFactsSnapshot,
) -> bool {
    // Fills may make the current exchange order set a strict subset of the last checkpoint.
    // Every still-open identity must remain owned; missing identities are settled later by exact
    // historical order readback before the reducer rolls or resets.
    if snapshot.orders.len() > state.owned_orders.len() {
        return false;
    }
    let Ok(mut expected) = state
        .owned_orders
        .keys()
        .map(client_order_id)
        .collect::<Result<BTreeSet<_>, _>>()
    else {
        return false;
    };
    snapshot
        .orders
        .iter()
        .all(|order| match &order.client_order_id {
            FieldState::Known(client_order_id) => CommandId::new(client_order_id)
                .ok()
                .is_some_and(|client_order_id| expected.remove(&client_order_id)),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => false,
        })
}
