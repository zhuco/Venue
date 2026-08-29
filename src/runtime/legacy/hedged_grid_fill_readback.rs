use super::*;

pub(super) enum MissingOwnedOrders {
    Filled(Vec<OwnedGridFill>),
    Rebuild,
}

pub(super) fn confirmed_missing_owned_fills(
    state: &HedgedGridState,
    commands: &CommandJournal,
    transport: &BinancePrivateFactsTransport,
    binding: &HedgedGridBinding,
    snapshot: &PrivateFactsSnapshot,
    private_generation: u64,
) -> Result<MissingOwnedOrders, HedgedGridLiveError> {
    if !snapshot_contains_only_owned_orders(state, snapshot) {
        return Err(HedgedGridLiveError::ForeignOrders);
    }
    let open_client_ids = snapshot
        .orders
        .iter()
        .filter_map(|order| match &order.client_order_id {
            FieldState::Known(value) => Some(value.as_str()),
            FieldState::Missing
            | FieldState::Null
            | FieldState::Unavailable { .. }
            | FieldState::NotApplicable => None,
        })
        .collect::<BTreeSet<_>>();
    let mut missing = Vec::new();
    for key in state.owned_orders.keys() {
        let client_id = client_order_id(key)?;
        if open_client_ids.contains(client_id.as_str()) {
            continue;
        }
        let Some(command_id) = commands.command_id_by_client_id(&client_id) else {
            return Ok(MissingOwnedOrders::Rebuild);
        };
        let Some(receipt) = commands.receipt(command_id) else {
            return Err(HedgedGridLiveError::Unresolved);
        };
        let CommandState::Accepted { venue_order_id } = &receipt.state else {
            return match receipt.state {
                CommandState::Rejected { .. } => Ok(MissingOwnedOrders::Rebuild),
                CommandState::Prepared
                | CommandState::Submitted
                | CommandState::Unknown { .. }
                | CommandState::Accepted { .. } => Err(HedgedGridLiveError::Unresolved),
            };
        };
        missing.push((key.clone(), client_id, venue_order_id.clone()));
    }

    let mut confirmed = Vec::new();
    let private = transport.private_rest();
    let readbacks = thread::scope(|scope| {
        let handles = missing
            .iter()
            .cloned()
            .map(|(key, client_id, expected_order_id)| {
                scope.spawn(move || {
                    let payload =
                        private.order_by_client_id(&binding.symbol, client_id.as_str())?;
                    let order = binance_private::parse_order(&payload, &binding.symbol)?;
                    Ok::<_, HedgedGridLiveError>((key, client_id, expected_order_id, order))
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().map_err(|_| HedgedGridLiveError::Dispatch)?)
            .collect::<Result<Vec<_>, _>>()
    })?;
    for (key, client_id, expected_order_id, order) in readbacks {
        if order.order_id != expected_order_id
            || !matches!(
                &order.client_order_id,
                FieldState::Known(value) if value == client_id.as_str()
            )
        {
            return Err(HedgedGridLiveError::Unresolved);
        }
        match order.state {
            OrderState::Filled => {
                let matching_fills = snapshot
                    .fills
                    .iter()
                    .filter(|fill| fill.order_id == order.order_id)
                    .collect::<Vec<_>>();
                if matching_fills.is_empty() {
                    return Err(HedgedGridLiveError::FillLiquidityUnknown);
                }
                let expected_quantity = state
                    .owned_orders
                    .get(&key)
                    .map(|intent| intent.quantity)
                    .ok_or(HedgedGridLiveError::Unresolved)?;
                let terminal_fill = match super::super::hedged_grid::terminal_owned_execution(
                    &matching_fills,
                    expected_quantity,
                ) {
                    Ok(fill) => fill,
                    Err(super::super::hedged_grid::TerminalExecutionError::Liquidity)
                        if matching_fills.iter().any(|fill| {
                            matches!(
                                super::super::hedged_grid::route_grid_fill(fill),
                                super::super::hedged_grid::GridFillRoute::TakerInventoryOnly
                            )
                        }) =>
                    {
                        return Err(HedgedGridLiveError::PostOnlyFillBecameTaker);
                    }
                    Err(_) => return Err(HedgedGridLiveError::FillLiquidityUnknown),
                };
                let FieldState::Known(execution_sequence) = terminal_fill.execution_sequence else {
                    return Err(HedgedGridLiveError::FillLiquidityUnknown);
                };
                confirmed.push((
                    execution_sequence,
                    OwnedGridFill {
                        fill_id: terminal_fill.fill_id.clone(),
                        private_generation,
                        source_order: key,
                        fill_price: terminal_fill.price,
                        complete: true,
                        maker: terminal_fill.maker.clone(),
                    },
                ));
            }
            OrderState::Cancelled | OrderState::Expired | OrderState::Rejected => {
                return Ok(MissingOwnedOrders::Rebuild);
            }
            OrderState::New | OrderState::PartiallyFilled | OrderState::Unknown => {
                return Err(HedgedGridLiveError::Unresolved);
            }
        }
    }
    Ok(MissingOwnedOrders::Filled(order_confirmed_fills(
        confirmed,
    )?))
}

pub(super) fn order_confirmed_fills(
    mut confirmed: Vec<(u64, OwnedGridFill)>,
) -> Result<Vec<OwnedGridFill>, HedgedGridLiveError> {
    confirmed.sort_by_key(|(execution_sequence, _)| *execution_sequence);
    if confirmed
        .windows(2)
        .any(|window| window[0].0 == window[1].0)
    {
        return Err(HedgedGridLiveError::FillLiquidityUnknown);
    }
    Ok(confirmed.into_iter().map(|(_, fill)| fill).collect())
}
