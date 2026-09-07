use super::store::{GridOrderRow, StrategyGridRecord};
use crate::multi_venue_store::MultiVenueStoreError as Error;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use venue_domain::{
    Asset, CancelCommand, CommandId, ExecutionCommand, LimitTimeInForce, MarketOrderCommand,
    MarketReduceCommand, OrderCommand, OrderOwner, OrderPurpose, OrderState, PositionSide, Price,
};
use venue_execution::{DurableMarketFacts, DurableOrderObservation, SignedAccountSnapshot};
use venue_strategies::hedged_grid::*;

pub(crate) struct GridWork {
    pub commands: Vec<(ExecutionCommand, Option<GridOrderIntent>)>,
    pub observations: Vec<(String, Decimal, bool)>,
    pub desired: Vec<GridOrderIntent>,
    pub anchor: Option<GridRollingAnchor>,
    pub lifecycle: Option<String>,
    pub new_revision: bool,
}

pub(crate) fn plan(
    record: &StrategyGridRecord,
    orders: &[GridOrderRow],
    snapshot: &SignedAccountSnapshot,
    market: &DurableMarketFacts,
    observations: &[DurableOrderObservation],
    now: u64,
) -> Result<GridWork, Error> {
    if snapshot.binding() != &market.binding
        || snapshot.binding().trading_account_id != record.trading_account_id
        || snapshot.binding().venue != record.venue
        || snapshot.binding().symbol != record.symbol
        || now < snapshot.observed_at_ms()
        || now - snapshot.observed_at_ms() > 5_000
        || now < market.observed_at_ms
        || now - market.observed_at_ms > 5_000
    {
        return Err(Error::Conflict);
    }
    let mut work = GridWork {
        commands: vec![],
        observations: vec![],
        desired: vec![],
        anchor: record.rolling_anchor.clone(),
        lifecycle: None,
        new_revision: false,
    };
    let mut actual = Vec::new();
    let mut hints = Vec::new();
    let mut resting = Vec::new();
    let mut external = GridCloseReservations::default();
    let by_client: BTreeMap<_, _> = observations
        .iter()
        .map(|o| (o.client_order_id.as_str(), o))
        .collect();
    if by_client.len() != observations.len() {
        return Err(Error::Conflict);
    }
    for row in orders {
        let client = row
            .command
            .native_client_id()
            .ok_or(Error::Conflict)?
            .as_str();
        let (state, filled) = if matches!(row.ledger_state.as_str(), "rejected" | "cancelled")
            && row.native_id.is_none()
        {
            (OrderState::Rejected, Decimal::ZERO)
        } else {
            let observed = by_client.get(client).ok_or(Error::Conflict)?;
            if row
                .native_id
                .as_deref()
                .is_some_and(|native| native != observed.native_order_id)
                || observed.filled_quantity < row.observed_filled
                || observed.filled_quantity > row.intent.quantity
                || observed.state == OrderState::Unknown
            {
                return Err(Error::Conflict);
            }
            (observed.state, observed.filled_quantity)
        };
        let terminal = matches!(
            state,
            OrderState::Filled | OrderState::Cancelled | OrderState::Expired | OrderState::Rejected
        );
        if state == OrderState::Filled && filled != row.intent.quantity {
            return Err(Error::Conflict);
        }
        work.observations
            .push((client.to_owned(), filled, terminal));
        if !terminal {
            // The aggregate historical query and current account snapshot must agree. A race
            // triggers a fresh cycle rather than planning against a fabricated empty surface.
            let native = by_client
                .get(client)
                .ok_or(Error::Conflict)?
                .native_order_id
                .as_str();
            let fact = snapshot
                .open_orders()
                .iter()
                .find(|o| o.venue_order_id.as_deref() == Some(native))
                .ok_or(Error::Conflict)?;
            if fact.filled_quantity != Some(filled)
                || fact.quantity != row.intent.quantity
                || fact.limit_price != Some(row.intent.price.value())
                || fact.side != row.intent.side
                || fact.reduce_only != row.intent.reduce_only
                || fact.time_in_force != Some(LimitTimeInForce::PostOnly)
            {
                return Err(Error::Conflict);
            }
            let mut remaining = row.intent.clone();
            remaining.quantity = remaining
                .quantity
                .checked_sub(filled)
                .ok_or(Error::Conflict)?;
            if remaining.quantity <= Decimal::ZERO {
                return Err(Error::Conflict);
            }
            actual.push(remaining);
            resting.push(row);
        }
        if filled > row.observed_filled && record.lifecycle == "running" {
            // PostOnly identity was signed-readback verified by the adapter. Cumulative quantity
            // and the consumed cursor are committed in the same transaction as the new surface.
            hints.push(GridMakerFill {
                fill_id: format!("{client}:{}", filled.normalize()),
                source_order: row.intent.clone(),
                complete: state == OrderState::Filled,
                maker: true,
            });
        }
    }
    for fact in snapshot.open_orders().iter().filter(|o| {
        o.symbol == record.symbol
            && o.reduce_only
            && o.family == venue_domain::NativeOrderFamily::UmOrder
    }) {
        let owned = orders.iter().any(|r| {
            r.native_id
                .as_deref()
                .is_some_and(|id| fact.venue_order_id.as_deref() == Some(id))
        });
        if owned {
            continue;
        }
        let remaining = fact
            .quantity
            .checked_sub(fact.filled_quantity.ok_or(Error::Conflict)?)
            .filter(|q| *q >= Decimal::ZERO)
            .ok_or(Error::Conflict)?;
        let target = match fact.position_side {
            PositionSide::Long => &mut external.long_quantity,
            PositionSide::Short => &mut external.short_quantity,
            PositionSide::Net => match record.config.net_direction {
                Some(GridPosition::Long) => &mut external.long_quantity,
                Some(GridPosition::Short) => &mut external.short_quantity,
                None => return Err(Error::Conflict),
            },
        };
        *target = target.checked_add(remaining).ok_or(Error::Conflict)?;
    }
    let cancel_all = |work: &mut GridWork| -> Result<(), Error> {
        for row in &resting {
            work.commands
                .push((cancel(record, row, work.commands.len())?, None));
        }
        Ok(())
    };
    if record.lifecycle != "running" {
        cancel_all(&mut work)?;
        if resting.is_empty() {
            work.anchor = None;
            work.new_revision = true;
            work.lifecycle = Some(
                match record.lifecycle.as_str() {
                    "pausing" => "paused",
                    "stopping" => "stopped",
                    "resetting" => "running",
                    _ => return Err(Error::Conflict),
                }
                .into(),
            );
        }
        return Ok(work);
    }
    let (long, short, mark) = inventory(record, snapshot, market.reference_price)?;
    let max_qty = market.maximum_quantity.unwrap_or(aligned_limit(
        {
            record
                .config
                .planner
                .maximum_grid_notional
                .value
                .checked_div(market.metadata.price.minimum)
                .unwrap_or(Decimal::ZERO)
        },
        market.metadata.quantity.step,
    )?);
    let max_price = market.maximum_price.unwrap_or(
        Price::new(aligned_limit(
            Decimal::MAX / Decimal::from(1_000),
            market.metadata.price.step,
        )?)
        .map_err(|_| Error::Invalid)?,
    );
    let mut instrument = market.metadata.clone();
    // Adapter sessions rotate generations without changing rules. The persisted anchor follows
    // rule content, while a changed precision, bound, or trading status still requires a reset.
    instrument.instrument.generation = 1;
    let rule_bytes =
        serde_json::to_vec(&(&instrument, market.maximum_quantity, market.maximum_price))
            .map_err(|_| Error::Invalid)?;
    let digest = Sha256::digest(rule_bytes);
    let mut generation = [0_u8; 8];
    generation.copy_from_slice(&digest[..8]);
    instrument.instrument.generation = u64::from_be_bytes(generation).max(1);
    let input = GridPlannerInput {
        net_direction: record.config.net_direction,
        config: record.config.planner.clone(),
        instrument,
        instrument_limits: GridInstrumentLimits {
            minimum_quantity: market.metadata.quantity.minimum,
            maximum_quantity: max_qty,
            minimum_price: Price::new(market.metadata.price.minimum).map_err(|_| Error::Invalid)?,
            maximum_price: max_price,
        },
        book: None,
        reference_price: Some(GridReferencePrice {
            price: market.reference_price,
            observed_at_ms: market.observed_at_ms,
        }),
        inventory: GridInventory {
            private_generation: snapshot.private_generation(),
            private_observed_at_ms: snapshot.observed_at_ms(),
            mark_price: mark,
            long_quantity: long,
            short_quantity: short,
        },
        owned_orders: actual.clone(),
        maker_fills: hints,
        pending_place_keys: BTreeSet::new(),
        other_close_reservations: external,
        rolling_anchor: record.rolling_anchor.clone(),
        convergence: GridConvergenceFacts::default(),
        risk: risk(record, snapshot, long, short, mark)?,
        control: GridPlannerControl::Run,
        now_ms: now,
    };
    match GridPlanner::plan(&input)
        .map_err(|_| Error::Invalid)?
        .directive
    {
        GridPlanDirective::Converge {
            rolling_anchor,
            desired_orders,
        } => {
            work.anchor = Some(rolling_anchor);
            work.desired = desired_orders.clone();
            // Remove conflicting resting reservations before replacing them. A cancel's signed
            // terminal readback fences the next account turn, including cancel/fill races.
            for row in &resting {
                let current = actual
                    .iter()
                    .find(|v| v.key == row.intent.key)
                    .ok_or(Error::Conflict)?;
                if !desired_orders.iter().any(|v| v == current) {
                    work.commands
                        .push((cancel(record, row, work.commands.len())?, None));
                }
            }
            for intent in desired_orders {
                if !actual.contains(&intent) {
                    let command = place(record, &intent, work.commands.len())?;
                    work.commands.push((command, Some(intent)));
                }
            }
        }
        GridPlanDirective::Replenish { adjustments, .. } => {
            cancel_all(&mut work)?;
            work.anchor = None;
            if resting.is_empty() {
                for a in adjustments {
                    let id = identity(record, "market", work.commands.len())?;
                    work.commands.push((
                        ExecutionCommand::PlaceMarket(MarketOrderCommand {
                            command_id: id.clone(),
                            client_order_id: id,
                            owner: owner(record, OrderPurpose::Entry),
                            position_side: leg(record, a.position),
                            side: a.side,
                            quantity: a.quantity,
                            reduce_only: false,
                        }),
                        None,
                    ));
                }
            }
        }
        GridPlanDirective::ReduceExposure { reductions, .. } => {
            cancel_all(&mut work)?;
            work.anchor = None;
            if resting.is_empty() {
                for a in reductions {
                    let id = identity(record, "reduce", work.commands.len())?;
                    work.commands.push((
                        ExecutionCommand::MarketReduce(MarketReduceCommand {
                            command_id: id.clone(),
                            client_order_id: id.clone(),
                            owner: owner(record, OrderPurpose::ExposureTakeProfit),
                            position_side: leg(record, a.position),
                            side: a.side,
                            quantity: a.quantity,
                            risk_episode_id: id,
                            position_generation: snapshot.private_generation(),
                        }),
                        None,
                    ));
                }
            }
        }
        GridPlanDirective::ResetRequired { .. } => {
            cancel_all(&mut work)?;
            work.lifecycle = Some("resetting".into());
            work.anchor = None;
        }
        GridPlanDirective::Blocked { .. } => return Err(Error::Conflict),
        GridPlanDirective::Stop { .. } => return Err(Error::Conflict),
    }
    if work.commands.len() > crate::multi_venue_store::MAX_STRATEGY_QUEUE_DEPTH {
        return Err(Error::Invalid);
    }
    Ok(work)
}

fn aligned_limit(value: Decimal, step: Decimal) -> Result<Decimal, Error> {
    // These are bridge arithmetic bounds only when the venue publishes no upper filter.
    // Keep the optional adapter fact unchanged and obey the core planner's aligned contract.
    if value <= Decimal::ZERO || step <= Decimal::ZERO {
        return Err(Error::Invalid);
    }
    value
        .checked_rem(step)
        .and_then(|remainder| value.checked_sub(remainder))
        .filter(|v| *v > Decimal::ZERO)
        .ok_or(Error::Invalid)
}

fn identity(record: &StrategyGridRecord, purpose: &str, index: usize) -> Result<CommandId, Error> {
    let raw = format!(
        "{}:{}:{}:{purpose}:{index}",
        record.instance_id, record.revision, record.plan_sequence
    );
    let digest = Sha256::digest(raw.as_bytes());
    // Keep new client IDs within the shared 28-byte admission boundary. Persisted IDs stay intact.
    let hex: String = digest[..13].iter().map(|b| format!("{b:02x}")).collect();
    CommandId::new(format!("sg{hex}")).map_err(|_| Error::Invalid)
}
fn owner(record: &StrategyGridRecord, purpose: OrderPurpose) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: record.instance_id.clone(),
        run_id: format!("r{}", record.revision),
        exchange: record.venue.as_str().into(),
        account: record.trading_account_id.clone(),
        symbol: record.symbol.clone(),
        purpose,
    }
}
fn leg(record: &StrategyGridRecord, p: GridPosition) -> PositionSide {
    if record.config.net_direction.is_some() {
        PositionSide::Net
    } else {
        match p {
            GridPosition::Long => PositionSide::Long,
            GridPosition::Short => PositionSide::Short,
        }
    }
}
fn place(
    record: &StrategyGridRecord,
    intent: &GridOrderIntent,
    index: usize,
) -> Result<ExecutionCommand, Error> {
    let id = identity(record, "place", index)?;
    Ok(ExecutionCommand::PlaceLimit(OrderCommand {
        command_id: id.clone(),
        client_order_id: id,
        owner: owner(
            record,
            if intent.reduce_only {
                OrderPurpose::Reduce
            } else {
                OrderPurpose::Entry
            },
        ),
        side: intent.side,
        position_side: leg(record, intent.key.position),
        quantity: intent.quantity,
        limit_price: intent.price,
        time_in_force: LimitTimeInForce::PostOnly,
        reduce_only: intent.reduce_only,
    }))
}
fn cancel(
    record: &StrategyGridRecord,
    row: &GridOrderRow,
    index: usize,
) -> Result<ExecutionCommand, Error> {
    Ok(ExecutionCommand::Cancel(CancelCommand {
        command_id: identity(record, "cancel", index)?,
        owner: row.command.mutation_owner().clone(),
        target_client_order_id: row
            .command
            .native_client_id()
            .ok_or(Error::Conflict)?
            .clone(),
    }))
}
fn inventory(
    record: &StrategyGridRecord,
    snapshot: &SignedAccountSnapshot,
    reference: Price,
) -> Result<(Decimal, Decimal, Price), Error> {
    let mut long = Decimal::ZERO;
    let mut short = Decimal::ZERO;
    let mut mark = None;
    let mut legs = BTreeSet::new();
    for p in snapshot
        .positions()
        .iter()
        .filter(|p| p.symbol == record.symbol)
    {
        if !legs.insert(p.position_side) {
            return Err(Error::Conflict);
        }
        if let Some(m) = p.mark_price {
            let m = Price::new(m).map_err(|_| Error::Conflict)?;
            if mark.is_some_and(|old| old != m) {
                return Err(Error::Conflict);
            }
            mark = Some(m);
        }
        match p.position_side {
            PositionSide::Long if record.config.net_direction.is_none() => long = p.quantity.abs(),
            PositionSide::Short if record.config.net_direction.is_none() => {
                short = p.quantity.abs()
            }
            PositionSide::Net if record.config.net_direction.is_some() => {
                let direction = record.config.net_direction.ok_or(Error::Conflict)?;
                if !p.quantity.is_zero()
                    && (p.quantity.is_sign_positive() != (direction == GridPosition::Long))
                {
                    return Err(Error::Conflict);
                }
                match direction {
                    GridPosition::Long => long = p.quantity.abs(),
                    GridPosition::Short => short = p.quantity.abs(),
                }
            }
            _ => return Err(Error::Conflict),
        }
    }
    // The reference value is a separately verified mark only when no inventory exists; nonzero
    // positions always require their own signed mark observation.
    if mark.is_none() && (!long.is_zero() || !short.is_zero()) {
        return Err(Error::Conflict);
    }
    Ok((long, short, mark.unwrap_or(reference)))
}
fn risk(
    record: &StrategyGridRecord,
    snapshot: &SignedAccountSnapshot,
    long: Decimal,
    short: Decimal,
    mark: Price,
) -> Result<Option<GridRiskFacts>, Error> {
    if record.config.planner.profit_reduction.is_none() {
        return Ok(None);
    }
    let quote = Asset::new(record.symbol.quote()).map_err(|_| Error::Invalid)?;
    let balances: Vec<_> = snapshot
        .balances()
        .iter()
        .filter(|b| b.asset == quote)
        .collect();
    let [balance] = balances.as_slice() else {
        return Err(Error::Conflict);
    };
    let mut legs = Vec::new();
    for (position, quantity) in [(GridPosition::Long, long), (GridPosition::Short, short)] {
        if quantity.is_zero() {
            continue;
        }
        let p = snapshot
            .positions()
            .iter()
            .find(|p| p.symbol == record.symbol && (p.position_side == leg(record, position)))
            .ok_or(Error::Conflict)?;
        let entry = p.entry_price.ok_or(Error::Conflict)?;
        let difference = match position {
            GridPosition::Long => mark.value().checked_sub(entry),
            GridPosition::Short => entry.checked_sub(mark.value()),
        }
        .ok_or(Error::Conflict)?;
        legs.push(venue_domain::LegRiskSnapshot {
            symbol: record.symbol.clone(),
            position_side: match position {
                GridPosition::Long => PositionSide::Long,
                GridPosition::Short => PositionSide::Short,
            },
            quantity,
            mark_price: mark,
            contract_multiplier: Decimal::ONE,
            notional: quantity.checked_mul(mark.value()).ok_or(Error::Conflict)?,
            unrealized_pnl: quantity.checked_mul(difference).ok_or(Error::Conflict)?,
            risk_currency: quote.clone(),
            private_generation: snapshot.private_generation(),
            observed_at_ms: snapshot.observed_at_ms(),
        });
    }
    Ok(Some(GridRiskFacts {
        account: venue_domain::AccountRiskSnapshot {
            exchange: record.venue.as_str().into(),
            account: record.trading_account_id.clone(),
            risk_currency: quote.clone(),
            account_equity: balance.equity,
            private_generation: snapshot.private_generation(),
            observed_at_ms: snapshot.observed_at_ms(),
            source_status: venue_domain::RiskSourceStatus::Complete,
        },
        legs,
        conversion: GridRiskConversion {
            risk_currency: quote.clone(),
            quote_currency: quote,
            quote_per_risk_unit: Decimal::ONE,
            private_generation: snapshot.private_generation(),
            observed_at_ms: snapshot.observed_at_ms(),
        },
    }))
}
