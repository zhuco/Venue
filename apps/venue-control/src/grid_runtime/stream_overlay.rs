use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use venue_control_protocol::{
    grid::{GridInstanceState, GridOrderRole},
    kol::{
        TerminalAccountProjection, TerminalFill, TerminalOpenOrder, TerminalOrderState,
        TerminalPositionMode,
    },
};
use venue_domain::domain::{FieldState, OrderState, PositionSide};
use venue_gateway_binance::BinancePrivateFillEvent;

use crate::{GridFillAllocation, GridOrderOwnership, GridOwnedOrderState, GridRuntimeRecord};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct AppliedGridStreamFill {
    pub(super) allocation: GridFillAllocation,
    pub(super) complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GridStreamOverlay {
    pub(super) projection: TerminalAccountProjection,
    pub(super) owners: Vec<GridOrderOwnership>,
    pub(super) fills: Vec<AppliedGridStreamFill>,
    pub(super) latest_event_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum GridStreamOverlayError {
    #[error("the signed Grid overlay baseline is invalid")]
    Baseline,
    #[error("the Grid order ownership surface is invalid")]
    Ownership,
    #[error("the authenticated Grid fill evidence is incomplete or conflicting")]
    Event,
    #[error("the authenticated Grid fill sequence is not monotonic")]
    Sequence,
    #[error("the authenticated Grid fill cumulative quantity conflicts with its baseline")]
    Cumulative,
}

pub(super) fn apply_stream_overlay(
    record: &GridRuntimeRecord,
    baseline: &TerminalAccountProjection,
    owners: &[GridOrderOwnership],
    events: &[BinancePrivateFillEvent],
) -> Result<GridStreamOverlay, GridStreamOverlayError> {
    apply_stream_overlay_inner(record, baseline, owners, events, false)
}

/// The caller must bind this continuation to the cached record revision, predecessor batch,
/// private/socket generations and monotonic event sequence before allowing a dirty record.
pub(super) fn apply_stream_continuation(
    record: &GridRuntimeRecord,
    baseline: &TerminalAccountProjection,
    owners: &[GridOrderOwnership],
    events: &[BinancePrivateFillEvent],
) -> Result<GridStreamOverlay, GridStreamOverlayError> {
    apply_stream_overlay_inner(record, baseline, owners, events, true)
}

fn apply_stream_overlay_inner(
    record: &GridRuntimeRecord,
    baseline: &TerminalAccountProjection,
    owners: &[GridOrderOwnership],
    events: &[BinancePrivateFillEvent],
    validated_continuation: bool,
) -> Result<GridStreamOverlay, GridStreamOverlayError> {
    validate_baseline(record, baseline, events, validated_continuation)?;
    let owner_indices = validate_owners(record, baseline, owners)?;

    let mut projection = baseline.clone();
    let mut owners = owners.to_vec();
    let mut applied = Vec::with_capacity(events.len());
    let mut batch_ids = BTreeMap::<String, &BinancePrivateFillEvent>::new();
    let mut last_sequence = None;
    let mut last_received_ms = None;
    let mut last_occurred_ms = None;
    let mut latest_event_ms = 0_u64;
    let stream_generation = events
        .first()
        .map(|event| event.stream_private_generation)
        .ok_or(GridStreamOverlayError::Event)?;

    for event in events {
        latest_event_ms = latest_event_ms.max(event.received_at_ms);
        if let Some(previous) = batch_ids.get(&event.fill.fill_id) {
            if *previous == event {
                continue;
            }
            return Err(GridStreamOverlayError::Event);
        }
        batch_ids.insert(event.fill.fill_id.clone(), event);

        let evidence = validate_event(
            record,
            baseline,
            event,
            stream_generation,
            &mut last_sequence,
            &mut last_received_ms,
            &mut last_occurred_ms,
        )?;
        let owner_index = *owner_indices
            .get(evidence.client_order_id)
            .ok_or(GridStreamOverlayError::Ownership)?;
        validate_current_working_owner(record, &owners[owner_index])?;
        let existing_fill = matching_existing_fill(&projection, event, evidence.position_side)?;
        if already_applied(
            &projection,
            &owners[owner_index],
            evidence.client_order_id,
            evidence.cumulative,
            evidence.state,
            existing_fill,
        )? {
            continue;
        }
        let has_existing_fill = existing_fill.is_some();

        let order_index = unique_open_order_index(&projection, evidence.client_order_id)?
            .ok_or(GridStreamOverlayError::Ownership)?;
        validate_event_owner(
            record,
            &owners[owner_index],
            &projection.open_orders[order_index],
            event,
            &evidence,
        )?;
        let expected_cumulative = owners[owner_index]
            .filled_quantity
            .checked_add(event.fill.quantity)
            .ok_or(GridStreamOverlayError::Cumulative)?;
        if evidence.cumulative != expected_cumulative
            || evidence.cumulative <= owners[owner_index].filled_quantity
        {
            return Err(GridStreamOverlayError::Cumulative);
        }

        apply_position_delta(
            &mut projection,
            &owners[owner_index],
            event.fill.quantity,
            event.fill.price,
        )?;
        owners[owner_index].filled_quantity = evidence.cumulative;
        owners[owner_index].last_seen_ms =
            owners[owner_index].last_seen_ms.max(event.received_at_ms);

        let complete = evidence.state == OrderState::Filled;
        if complete {
            owners[owner_index].state = GridOwnedOrderState::Terminal;
            projection.open_orders.remove(order_index);
        } else {
            let order = projection
                .open_orders
                .get_mut(order_index)
                .ok_or(GridStreamOverlayError::Ownership)?;
            order.filled_quantity = Some(evidence.cumulative);
            order.state = TerminalOrderState::PartiallyFilled;
        }

        if !has_existing_fill {
            projection
                .fills
                .push(terminal_fill(event, evidence.position_side));
        }
        applied.push(AppliedGridStreamFill {
            allocation: GridFillAllocation {
                instance_id: record.instance.instance_id.clone(),
                trading_account_id: record.instance.trading_account_id.clone(),
                config_revision: owners[owner_index].config_revision,
                client_order_id: evidence.client_order_id.to_owned(),
                native_trade_id: event.fill.fill_id.clone(),
                symbol: event.fill.symbol.clone(),
                position_side: evidence.position_side,
                role: owners[owner_index].key.role,
                quantity: event.fill.quantity,
                price: event.fill.price.value(),
                maker: Some(true),
                occurred_ms: event.fill.exchange_time_ms,
                observed_ms: event.received_at_ms,
            },
            complete,
        });
    }

    projection
        .validate()
        .map_err(|_| GridStreamOverlayError::Baseline)?;
    validate_owners(record, &projection, &owners)?;
    Ok(GridStreamOverlay {
        projection,
        owners,
        fills: applied,
        latest_event_ms,
    })
}

struct EventEvidence<'a> {
    client_order_id: &'a str,
    position_side: PositionSide,
    cumulative: Decimal,
    state: OrderState,
}

fn validate_baseline(
    record: &GridRuntimeRecord,
    baseline: &TerminalAccountProjection,
    events: &[BinancePrivateFillEvent],
    validated_continuation: bool,
) -> Result<(), GridStreamOverlayError> {
    record
        .instance
        .validate()
        .map_err(|_| GridStreamOverlayError::Baseline)?;
    baseline
        .validate()
        .map_err(|_| GridStreamOverlayError::Baseline)?;
    if events.is_empty()
        || record.instance.state != GridInstanceState::Running
        || (record.instance.dirty && !validated_continuation)
        || baseline.position_mode != TerminalPositionMode::Hedge
        || baseline.credential_id != record.instance.credential_id
        || baseline.trading_account_id != record.instance.trading_account_id
    {
        return Err(GridStreamOverlayError::Baseline);
    }

    let mut sides = BTreeSet::new();
    for position in baseline
        .positions
        .iter()
        .filter(|position| position.symbol == record.instance.symbol)
    {
        if position.position_side == PositionSide::Net
            || position.quantity < Decimal::ZERO
            || !sides.insert(position.position_side)
        {
            return Err(GridStreamOverlayError::Baseline);
        }
    }
    if sides != BTreeSet::from([PositionSide::Long, PositionSide::Short]) {
        return Err(GridStreamOverlayError::Baseline);
    }
    Ok(())
}

fn validate_owners(
    record: &GridRuntimeRecord,
    projection: &TerminalAccountProjection,
    owners: &[GridOrderOwnership],
) -> Result<BTreeMap<String, usize>, GridStreamOverlayError> {
    let mut clients = BTreeMap::new();
    let mut native_ids = BTreeSet::new();
    let mut working_semantic_keys = BTreeSet::new();
    for (index, owner) in owners.iter().enumerate() {
        if owner.instance_id != record.instance.instance_id
            || owner.trading_account_id != record.instance.trading_account_id
            || owner.symbol != record.instance.symbol
            || owner.config_revision == 0
            || owner.config_revision > record.instance.config_revision
            || owner.plan_revision == 0
            || owner.plan_revision > record.instance.plan_revision
            || owner.key.validate().is_err()
            || owner.key.position_side == PositionSide::Net
            || owner.client_order_id.trim().is_empty()
            || owner.client_order_id.len() > 36
            || owner.place_command_id.trim().is_empty()
            || owner.quantity <= Decimal::ZERO
            || owner.filled_quantity < Decimal::ZERO
            || owner.filled_quantity > owner.quantity
            || owner.limit_price <= Decimal::ZERO
            || owner.first_seen_ms == 0
            || owner.last_seen_ms < owner.first_seen_ms
            || clients
                .insert(owner.client_order_id.clone(), index)
                .is_some()
            || owner.native_order_id.as_ref().is_some_and(|native| {
                native.trim().is_empty() || !native_ids.insert(native.clone())
            })
        {
            return Err(GridStreamOverlayError::Ownership);
        }
        let order = unique_open_order_index(projection, &owner.client_order_id)?;
        match (owner.state, order) {
            (GridOwnedOrderState::Working, Some(order)) => {
                if owner.native_order_id.is_none()
                    || owner.config_revision != record.instance.config_revision
                    || !working_semantic_keys.insert(owner.key.encoded())
                {
                    return Err(GridStreamOverlayError::Ownership);
                }
                validate_open_owner(owner, &projection.open_orders[order])?;
            }
            // A command rejected before submission never acquired a native ID. Historical
            // zero-fill terminal records cannot invalidate unrelated live order evidence.
            (GridOwnedOrderState::Terminal, None)
                if owner.native_order_id.is_some() || owner.filled_quantity.is_zero() => {}
            _ => return Err(GridStreamOverlayError::Ownership),
        }
    }
    Ok(clients)
}

#[allow(clippy::too_many_arguments)]
fn validate_event<'a>(
    record: &GridRuntimeRecord,
    baseline: &TerminalAccountProjection,
    event: &'a BinancePrivateFillEvent,
    stream_generation: u64,
    last_sequence: &mut Option<u64>,
    last_received_ms: &mut Option<u64>,
    last_occurred_ms: &mut Option<u64>,
) -> Result<EventEvidence<'a>, GridStreamOverlayError> {
    event
        .fill
        .validate()
        .map_err(|_| GridStreamOverlayError::Event)?;
    let client_order_id = match &event.client_order_id {
        FieldState::Known(value)
            if !value.trim().is_empty()
                && value.len() <= 36
                && !value.chars().any(char::is_whitespace) =>
        {
            value.as_str()
        }
        _ => return Err(GridStreamOverlayError::Event),
    };
    let position_side = match event.fill.position_side {
        FieldState::Known(side @ (PositionSide::Long | PositionSide::Short)) => side,
        _ => return Err(GridStreamOverlayError::Event),
    };
    let sequence = match event.fill.execution_sequence {
        FieldState::Known(value) if value > 0 => value,
        _ => return Err(GridStreamOverlayError::Event),
    };
    let occurred_ms = event
        .fill
        .exchange_time_ms
        .filter(|occurred| *occurred > 0 && *occurred <= event.received_at_ms)
        .ok_or(GridStreamOverlayError::Event)?;
    let (original, cumulative, state) = event
        .complete_order_progress()
        .ok_or(GridStreamOverlayError::Event)?;
    if event.stream_private_generation == 0
        || event.stream_private_generation != stream_generation
        || event.stream_private_generation > event.private_generation
        || event.private_generation != baseline.private_generation
        || event.received_at_ms <= baseline.observed_ms
        || event.fill.symbol != record.instance.symbol
        || event.fill.fill_id.trim().is_empty()
        || event.fill.order_id.trim().is_empty()
        || event.fill.price.value() <= Decimal::ZERO
        || event.fill.maker != FieldState::Known(true)
        || original <= Decimal::ZERO
        || cumulative <= Decimal::ZERO
        || cumulative > original
        || event.fill.quantity <= Decimal::ZERO
        || event.fill.quantity > cumulative
        || (state == OrderState::PartiallyFilled && cumulative >= original)
        || (state == OrderState::Filled && cumulative != original)
    {
        return Err(GridStreamOverlayError::Event);
    }
    if last_sequence.is_some_and(|previous| sequence <= previous)
        || last_received_ms.is_some_and(|previous| event.received_at_ms < previous)
        || last_occurred_ms.is_some_and(|previous| occurred_ms < previous)
    {
        return Err(GridStreamOverlayError::Sequence);
    }
    *last_sequence = Some(sequence);
    *last_received_ms = Some(event.received_at_ms);
    *last_occurred_ms = Some(occurred_ms);
    Ok(EventEvidence {
        client_order_id,
        position_side,
        cumulative,
        state,
    })
}

fn validate_event_owner(
    record: &GridRuntimeRecord,
    owner: &GridOrderOwnership,
    order: &TerminalOpenOrder,
    event: &BinancePrivateFillEvent,
    evidence: &EventEvidence<'_>,
) -> Result<(), GridStreamOverlayError> {
    validate_current_working_owner(record, owner)?;
    validate_open_owner(owner, order)?;
    let original = match event.original_quantity {
        FieldState::Known(value) => value,
        _ => return Err(GridStreamOverlayError::Event),
    };
    if owner.instance_id != record.instance.instance_id
        || owner.client_order_id != evidence.client_order_id
        || owner.native_order_id.as_deref() != Some(event.native_order_id())
        || order.native_order_id.as_deref() != Some(event.native_order_id())
        || owner.key.position_side != evidence.position_side
        || owner.key.order_side() != event.fill.side
        || owner.quantity != original
        || order.quantity != original
        || order.filled_quantity != Some(owner.filled_quantity)
    {
        return Err(GridStreamOverlayError::Ownership);
    }
    Ok(())
}

fn validate_current_working_owner(
    record: &GridRuntimeRecord,
    owner: &GridOrderOwnership,
) -> Result<(), GridStreamOverlayError> {
    if owner.instance_id != record.instance.instance_id
        || owner.config_revision != record.instance.config_revision
        || owner.plan_revision == 0
        || owner.plan_revision > record.instance.plan_revision
        || owner.state != GridOwnedOrderState::Working
    {
        return Err(GridStreamOverlayError::Ownership);
    }
    Ok(())
}

fn validate_open_owner(
    owner: &GridOrderOwnership,
    order: &TerminalOpenOrder,
) -> Result<(), GridStreamOverlayError> {
    let expected_state = if owner.filled_quantity.is_zero() {
        TerminalOrderState::New
    } else {
        TerminalOrderState::PartiallyFilled
    };
    if order.client_order_id != owner.client_order_id
        || order.native_order_id != owner.native_order_id
        || order.symbol != owner.symbol
        || order.order_side != owner.key.order_side()
        || order.position_side != owner.key.position_side
        || order.quantity != owner.quantity
        || order.filled_quantity != Some(owner.filled_quantity)
        || order.limit_price != Some(owner.limit_price)
        || !order.post_only
        || order.state != expected_state
        || owner.filled_quantity >= owner.quantity
    {
        return Err(GridStreamOverlayError::Ownership);
    }
    Ok(())
}

fn unique_open_order_index(
    projection: &TerminalAccountProjection,
    client_order_id: &str,
) -> Result<Option<usize>, GridStreamOverlayError> {
    let mut matches = projection
        .open_orders
        .iter()
        .enumerate()
        .filter(|(_, order)| order.client_order_id == client_order_id)
        .map(|(index, _)| index);
    let first = matches.next();
    if matches.next().is_some() {
        return Err(GridStreamOverlayError::Ownership);
    }
    Ok(first)
}

fn matching_existing_fill<'a>(
    projection: &'a TerminalAccountProjection,
    event: &BinancePrivateFillEvent,
    position_side: PositionSide,
) -> Result<Option<&'a TerminalFill>, GridStreamOverlayError> {
    let mut matches = projection.fills.iter().filter(|fill| {
        fill.symbol == event.fill.symbol && fill.native_trade_id == event.fill.fill_id
    });
    let existing = matches.next();
    if matches.next().is_some() {
        return Err(GridStreamOverlayError::Event);
    }
    if existing.is_some_and(|fill| *fill != terminal_fill(event, position_side)) {
        return Err(GridStreamOverlayError::Event);
    }
    Ok(existing)
}

fn already_applied(
    projection: &TerminalAccountProjection,
    owner: &GridOrderOwnership,
    client_order_id: &str,
    cumulative: Decimal,
    state: OrderState,
    existing_fill: Option<&TerminalFill>,
) -> Result<bool, GridStreamOverlayError> {
    if existing_fill.is_none() || owner.filled_quantity != cumulative {
        return Ok(false);
    }
    let order = unique_open_order_index(projection, client_order_id)?;
    match state {
        OrderState::PartiallyFilled => Ok(owner.state == GridOwnedOrderState::Working
            && order.is_some_and(|index| {
                projection.open_orders[index].filled_quantity == Some(cumulative)
                    && projection.open_orders[index].state == TerminalOrderState::PartiallyFilled
            })),
        OrderState::Filled => Ok(owner.state == GridOwnedOrderState::Terminal && order.is_none()),
        _ => Err(GridStreamOverlayError::Event),
    }
}

fn apply_position_delta(
    projection: &mut TerminalAccountProjection,
    owner: &GridOrderOwnership,
    quantity: Decimal,
    fill_price: venue_domain::domain::Price,
) -> Result<(), GridStreamOverlayError> {
    let mut matches = projection.positions.iter_mut().filter(|position| {
        position.symbol == owner.symbol && position.position_side == owner.key.position_side
    });
    let position = matches.next().ok_or(GridStreamOverlayError::Baseline)?;
    if matches.next().is_some() || position.quantity < Decimal::ZERO {
        return Err(GridStreamOverlayError::Baseline);
    }
    let next_quantity = match owner.key.role {
        GridOrderRole::Open => position.quantity.checked_add(quantity),
        GridOrderRole::Close => position.quantity.checked_sub(quantity),
    }
    .filter(|value| *value >= Decimal::ZERO)
    .ok_or(GridStreamOverlayError::Baseline)?;
    if owner.key.role == GridOrderRole::Open {
        let prior_cost = if position.quantity.is_zero() {
            Some(Decimal::ZERO)
        } else {
            position
                .entry_price
                .and_then(|price| price.checked_mul(position.quantity))
        };
        position.entry_price = Some(
            prior_cost
                .and_then(|cost| {
                    fill_price
                        .value()
                        .checked_mul(quantity)
                        .and_then(|fill| cost.checked_add(fill))
                })
                .and_then(|cost| cost.checked_div(next_quantity))
                .ok_or(GridStreamOverlayError::Baseline)?,
        );
    } else if next_quantity.is_zero() {
        position.entry_price = None;
    }
    position.quantity = next_quantity;
    Ok(())
}

fn terminal_fill(event: &BinancePrivateFillEvent, position_side: PositionSide) -> TerminalFill {
    TerminalFill {
        native_trade_id: event.fill.fill_id.clone(),
        native_order_id: event.fill.order_id.clone(),
        symbol: event.fill.symbol.clone(),
        order_side: event.fill.side,
        position_side,
        quantity: event.fill.quantity,
        price: event.fill.price.value(),
        maker: Some(true),
        occurred_ms: event.fill.exchange_time_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use venue_control_protocol::{
        grid::{
            GRID_SCHEMA_VERSION, GridConfig, GridInstanceSummary, GridInventoryReplenishment,
            GridProfitReduction, GridResetPolicy,
        },
        kol::{TERMINAL_PROJECTION_SCHEMA_VERSION, TerminalAsset, TerminalPosition},
    };
    use venue_domain::domain::{Fill, OrderSide, Price, Symbol};

    const INSTANCE_ID: &str = "00000000-0000-4000-8000-000000000001";
    const CREDENTIAL_ID: &str = "00000000-0000-4000-8000-000000000002";
    const ACCOUNT_ID: &str = "00000000-0000-4000-8000-000000000003";

    #[test]
    fn enabled_inventory_and_profit_policies_do_not_disable_stream_rolling()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::super::{
            ActualSurface, BinanceGridRuntime, fast_path::ordinary_stream_record_eligible,
            private_facts,
        };
        let mut record = record()?;
        record.instance.config.inventory_replenishment.enabled = true;
        record.instance.config.profit_reduction.enabled = true;
        assert!(ordinary_stream_record_eligible(&record, false));
        let projection = projection(&[])?;
        let actual = ActualSurface {
            ownership: BTreeMap::new(),
            orders: BTreeMap::new(),
            intents: vec![],
            other_close_reservations: Default::default(),
        };
        let private = private_facts(&record, &projection, &actual)?;
        let mut conversion = venue_gateway_binance::portfolio::UsdConversionEvidence {
            asset: "USDT".parse()?,
            usd_per_asset: Decimal::new(99, 2),
            private_generation: 3,
            observed_at_ms: 100,
            source_time_ms: 100,
        };
        let risk = BinanceGridRuntime::risk_from_conversion(
            &record,
            &projection,
            &private,
            conversion.clone(),
        )?;
        assert_eq!(risk.legs.len(), 2);
        assert_eq!(risk.legs[0].notional, Decimal::from(990));
        conversion.private_generation = 4;
        assert!(
            BinanceGridRuntime::risk_from_conversion(&record, &projection, &private, conversion)
                .is_err()
        );
        record.instance.dirty = true;
        assert!(!ordinary_stream_record_eligible(&record, false));
        assert!(ordinary_stream_record_eligible(&record, true));
        record.instance.state = GridInstanceState::Paused;
        assert!(!ordinary_stream_record_eligible(&record, true));
        Ok(())
    }

    #[test]
    fn streamed_open_preserves_weighted_entry_for_profit_risk()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = projection(&[])?;
        let owned = owner(
            "client",
            "native",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?;
        apply_position_delta(
            &mut projection,
            &owned,
            Decimal::from(10),
            Price::new(Decimal::from(120))?,
        )?;
        assert_eq!(projection.positions[0].quantity, Decimal::from(20));
        assert_eq!(
            projection.positions[0].entry_price,
            Some(Decimal::from(110))
        );
        Ok(())
    }

    fn symbol() -> Result<Symbol, Box<dyn std::error::Error>> {
        Ok("BTC/USDT".parse()?)
    }

    fn record() -> Result<GridRuntimeRecord, Box<dyn std::error::Error>> {
        Ok(GridRuntimeRecord {
            owner_user_id: "00000000-0000-4000-8000-000000000004".into(),
            tail_batch_id: None,
            instance: GridInstanceSummary {
                schema_version: GRID_SCHEMA_VERSION,
                instance_id: INSTANCE_ID.into(),
                credential_id: CREDENTIAL_ID.into(),
                trading_account_id: ACCOUNT_ID.into(),
                symbol: symbol()?,
                state: GridInstanceState::Running,
                revision: 1,
                config_revision: 2,
                plan_revision: 7,
                config: GridConfig {
                    order_notional: Decimal::from(5),
                    spacing_rate: Decimal::new(2, 3),
                    grid_levels: 2,
                    max_total_notional: Decimal::from(100),
                    inventory_replenishment: GridInventoryReplenishment {
                        enabled: false,
                        minimum_inventory_notional: Decimal::from(10),
                        target_inventory_notional: Decimal::from(20),
                        max_single_replenishment_notional: Decimal::from(10),
                    },
                    profit_reduction: GridProfitReduction {
                        enabled: false,
                        inventory_equity_multiple: Decimal::from(3),
                        minimum_unrealized_profit_rate: Decimal::new(5, 2),
                        reduction_fraction: Decimal::new(3, 1),
                        max_single_reduce_notional: Decimal::from(20),
                    },
                    reset_policy: GridResetPolicy {
                        stale_market_ms: 5_000,
                        stale_private_ms: 5_000,
                        convergence_timeout_ms: 10_000,
                        max_consecutive_failures: 3,
                    },
                },
                anchor: None,
                desired_digest: None,
                dirty: false,
                convergence_started_ms: None,
                consecutive_failures: 0,
                last_facts_ms: Some(100),
                attention_code: None,
                created_ms: 1,
                updated_ms: 100,
            },
        })
    }

    fn projection(
        orders: &[GridOrderOwnership],
    ) -> Result<TerminalAccountProjection, Box<dyn std::error::Error>> {
        let open_orders = orders
            .iter()
            .map(|owner| TerminalOpenOrder {
                client_order_id: owner.client_order_id.clone(),
                native_order_id: owner.native_order_id.clone(),
                symbol: owner.symbol.clone(),
                order_side: owner.key.order_side(),
                position_side: owner.key.position_side,
                quantity: owner.quantity,
                filled_quantity: Some(owner.filled_quantity),
                limit_price: Some(owner.limit_price),
                post_only: true,
                time_in_force: Some(venue_domain::LimitTimeInForce::PostOnly),
                reduce_only: false,
                state: if owner.filled_quantity.is_zero() {
                    TerminalOrderState::New
                } else {
                    TerminalOrderState::PartiallyFilled
                },
                created_ms: Some(1),
            })
            .collect();
        Ok(TerminalAccountProjection {
            schema_version: TERMINAL_PROJECTION_SCHEMA_VERSION,
            credential_id: CREDENTIAL_ID.into(),
            trading_account_id: ACCOUNT_ID.into(),
            observed_ms: 100,
            persisted_ms: 100,
            private_generation: 3,
            position_mode: TerminalPositionMode::Hedge,
            positions: vec![
                TerminalPosition {
                    symbol: symbol()?,
                    position_side: PositionSide::Long,
                    quantity: Decimal::from(10),
                    entry_price: Some(Decimal::from(100)),
                    mark_price: Some(Decimal::from(100)),
                },
                TerminalPosition {
                    symbol: symbol()?,
                    position_side: PositionSide::Short,
                    quantity: Decimal::from(10),
                    entry_price: Some(Decimal::from(100)),
                    mark_price: Some(Decimal::from(100)),
                },
            ],
            position_history: Vec::new(),
            open_orders,
            fills: Vec::new(),
            assets: vec![TerminalAsset {
                asset: "USD".into(),
                equity: Decimal::from(100),
                available_margin: Some(Decimal::from(100)),
            }],
        })
    }

    fn owner(
        client: &str,
        native: &str,
        position_side: PositionSide,
        role: GridOrderRole,
        sequence: u64,
    ) -> Result<GridOrderOwnership, Box<dyn std::error::Error>> {
        Ok(GridOrderOwnership {
            instance_id: INSTANCE_ID.into(),
            trading_account_id: ACCOUNT_ID.into(),
            config_revision: 2,
            plan_revision: 7,
            key: venue_control_protocol::grid::GridOrderSemanticKey {
                position_side,
                role,
                level: 1,
                sequence,
            },
            place_command_id: format!("place-{sequence}"),
            client_order_id: client.into(),
            symbol: symbol()?,
            quantity: Decimal::from(2),
            filled_quantity: Decimal::ZERO,
            limit_price: Decimal::from(100),
            native_order_id: Some(native.into()),
            state: GridOwnedOrderState::Working,
            first_seen_ms: 10,
            last_seen_ms: 100,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn event(
        trade: &str,
        native: &str,
        client: &str,
        sequence: u64,
        side: OrderSide,
        position_side: PositionSide,
        last: Decimal,
        cumulative: Decimal,
        state: OrderState,
    ) -> Result<BinancePrivateFillEvent, Box<dyn std::error::Error>> {
        Ok(BinancePrivateFillEvent {
            stream_private_generation: 3,
            private_generation: 3,
            received_at_ms: 109 + sequence,
            fill: Fill {
                fill_id: trade.into(),
                execution_sequence: FieldState::Known(sequence),
                order_id: native.into(),
                symbol: symbol()?,
                side,
                position_side: FieldState::Known(position_side),
                quantity: last,
                price: Price::new(Decimal::from(100))?,
                fee: FieldState::Missing,
                realized_pnl: FieldState::Missing,
                maker: FieldState::Known(true),
                exchange_time_ms: Some(108 + sequence),
            },
            client_order_id: FieldState::Known(client.into()),
            original_quantity: FieldState::Known(Decimal::from(2)),
            cumulative_filled_quantity: FieldState::Known(cumulative),
            order_state: FieldState::Known(state),
        })
    }

    fn owner_event(
        owner: &GridOrderOwnership,
        sequence: u64,
        last: Decimal,
        cumulative: Decimal,
        state: OrderState,
    ) -> Result<BinancePrivateFillEvent, Box<dyn std::error::Error>> {
        let native = owner
            .native_order_id
            .as_deref()
            .ok_or("test owner lacks native identity")?;
        event(
            &sequence.to_string(),
            native,
            &owner.client_order_id,
            sequence,
            owner.key.order_side(),
            owner.key.position_side,
            last,
            cumulative,
            state,
        )
    }

    fn position_quantity(
        projection: &TerminalAccountProjection,
        side: PositionSide,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        projection
            .positions
            .iter()
            .find(|position| position.position_side == side)
            .map(|position| position.quantity)
            .ok_or_else(|| "position is absent".into())
    }

    #[test]
    fn one_complete_fill_closes_order_and_updates_long_inventory()
    -> Result<(), Box<dyn std::error::Error>> {
        let owners = vec![owner(
            "client-long-open",
            "native-1",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?];
        let baseline = projection(&owners)?;
        let fill = owner_event(
            &owners[0],
            1,
            Decimal::from(2),
            Decimal::from(2),
            OrderState::Filled,
        )?;
        let overlaid = apply_stream_overlay(&record()?, &baseline, &owners, &[fill])?;
        assert!(overlaid.projection.open_orders.is_empty());
        assert_eq!(
            position_quantity(&overlaid.projection, PositionSide::Long)?,
            Decimal::from(12)
        );
        assert_eq!(overlaid.owners[0].state, GridOwnedOrderState::Terminal);
        assert_eq!(overlaid.fills.len(), 1);
        assert!(overlaid.fills[0].complete);
        assert_eq!(overlaid.latest_event_ms, 110);
        assert_eq!(overlaid.projection.observed_ms, baseline.observed_ms);
        assert_eq!(
            overlaid.projection.private_generation,
            baseline.private_generation
        );
        Ok(())
    }

    #[test]
    fn two_complete_fills_share_one_overlay_without_fixed_command_counts()
    -> Result<(), Box<dyn std::error::Error>> {
        let owners = vec![
            owner(
                "client-long-close",
                "native-1",
                PositionSide::Long,
                GridOrderRole::Close,
                1,
            )?,
            owner(
                "client-short-open",
                "native-2",
                PositionSide::Short,
                GridOrderRole::Open,
                2,
            )?,
        ];
        let baseline = projection(&owners)?;
        let events = owners
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                owner_event(
                    owner,
                    u64::try_from(index + 1)?,
                    Decimal::from(2),
                    Decimal::from(2),
                    OrderState::Filled,
                )
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        let overlaid = apply_stream_overlay(&record()?, &baseline, &owners, &events)?;
        assert!(overlaid.projection.open_orders.is_empty());
        assert_eq!(overlaid.fills.len(), 2);
        assert!(overlaid.fills.iter().all(|fill| fill.complete));
        assert_eq!(
            position_quantity(&overlaid.projection, PositionSide::Long)?,
            Decimal::from(8)
        );
        assert_eq!(
            position_quantity(&overlaid.projection, PositionSide::Short)?,
            Decimal::from(12)
        );
        Ok(())
    }

    #[test]
    fn partial_then_full_updates_remaining_before_completion()
    -> Result<(), Box<dyn std::error::Error>> {
        let owners = vec![owner(
            "client-short-close",
            "native-1",
            PositionSide::Short,
            GridOrderRole::Close,
            1,
        )?];
        let baseline = projection(&owners)?;
        let events = vec![
            owner_event(
                &owners[0],
                1,
                Decimal::new(5, 1),
                Decimal::new(5, 1),
                OrderState::PartiallyFilled,
            )?,
            owner_event(
                &owners[0],
                2,
                Decimal::new(15, 1),
                Decimal::from(2),
                OrderState::Filled,
            )?,
        ];
        let overlaid = apply_stream_overlay(&record()?, &baseline, &owners, &events)?;
        assert_eq!(
            overlaid
                .fills
                .iter()
                .map(|fill| fill.complete)
                .collect::<Vec<_>>(),
            vec![false, true]
        );
        assert_eq!(overlaid.owners[0].filled_quantity, Decimal::from(2));
        assert_eq!(
            position_quantity(&overlaid.projection, PositionSide::Short)?,
            Decimal::from(8)
        );
        assert!(overlaid.projection.open_orders.is_empty());
        Ok(())
    }

    #[test]
    fn out_of_order_events_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let owners = vec![
            owner(
                "client-1",
                "native-1",
                PositionSide::Long,
                GridOrderRole::Open,
                1,
            )?,
            owner(
                "client-2",
                "native-2",
                PositionSide::Short,
                GridOrderRole::Open,
                2,
            )?,
        ];
        let baseline = projection(&owners)?;
        let events = vec![
            owner_event(
                &owners[1],
                2,
                Decimal::from(2),
                Decimal::from(2),
                OrderState::Filled,
            )?,
            owner_event(
                &owners[0],
                1,
                Decimal::from(2),
                Decimal::from(2),
                OrderState::Filled,
            )?,
        ];
        assert_eq!(
            apply_stream_overlay(&record()?, &baseline, &owners, &events),
            Err(GridStreamOverlayError::Sequence)
        );
        Ok(())
    }

    #[test]
    fn cumulative_conflict_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let owners = vec![owner(
            "client-1",
            "native-1",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?];
        let baseline = projection(&owners)?;
        let fill = owner_event(
            &owners[0],
            1,
            Decimal::ONE,
            Decimal::new(15, 1),
            OrderState::PartiallyFilled,
        )?;
        assert_eq!(
            apply_stream_overlay(&record()?, &baseline, &owners, &[fill]),
            Err(GridStreamOverlayError::Cumulative)
        );
        Ok(())
    }

    #[test]
    fn non_maker_fill_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let owners = vec![owner(
            "client-1",
            "native-1",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?];
        let baseline = projection(&owners)?;
        let mut fill = owner_event(
            &owners[0],
            1,
            Decimal::from(2),
            Decimal::from(2),
            OrderState::Filled,
        )?;
        fill.fill.maker = FieldState::Known(false);
        assert_eq!(
            apply_stream_overlay(&record()?, &baseline, &owners, &[fill]),
            Err(GridStreamOverlayError::Event)
        );
        Ok(())
    }

    #[test]
    fn unknown_owner_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let owners = vec![owner(
            "client-1",
            "native-1",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?];
        let baseline = projection(&owners)?;
        let mut fill = owner_event(
            &owners[0],
            1,
            Decimal::from(2),
            Decimal::from(2),
            OrderState::Filled,
        )?;
        fill.client_order_id = FieldState::Known("unknown-client".into());
        assert_eq!(
            apply_stream_overlay(&record()?, &baseline, &owners, &[fill]),
            Err(GridStreamOverlayError::Ownership)
        );
        Ok(())
    }

    #[test]
    fn identical_trade_id_is_idempotent_within_batch() -> Result<(), Box<dyn std::error::Error>> {
        let owners = vec![owner(
            "client-1",
            "native-1",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?];
        let baseline = projection(&owners)?;
        let fill = owner_event(
            &owners[0],
            1,
            Decimal::from(2),
            Decimal::from(2),
            OrderState::Filled,
        )?;
        let overlaid = apply_stream_overlay(&record()?, &baseline, &owners, &[fill.clone(), fill])?;
        assert_eq!(overlaid.fills.len(), 1);
        assert_eq!(
            position_quantity(&overlaid.projection, PositionSide::Long)?,
            Decimal::from(12)
        );
        Ok(())
    }

    #[test]
    fn dirty_cross_microbatch_continuation_applies_second_leg_without_recovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let owners = vec![
            owner(
                "live-long",
                "native-long",
                PositionSide::Long,
                GridOrderRole::Open,
                1,
            )?,
            owner(
                "live-short",
                "native-short",
                PositionSide::Short,
                GridOrderRole::Close,
                2,
            )?,
        ];
        let baseline = projection(&owners)?;
        let first = owner_event(
            &owners[0],
            1,
            owners[0].quantity,
            owners[0].quantity,
            OrderState::Filled,
        )?;
        let second = owner_event(
            &owners[1],
            2,
            owners[1].quantity,
            owners[1].quantity,
            OrderState::Filled,
        )?;
        let mut record = record()?;
        let first_overlay = apply_stream_overlay(&record, &baseline, &owners, &[first])?;
        record.instance.dirty = true;
        assert_eq!(
            apply_stream_overlay(
                &record,
                &first_overlay.projection,
                &first_overlay.owners,
                std::slice::from_ref(&second)
            ),
            Err(GridStreamOverlayError::Baseline)
        );
        let second_overlay = apply_stream_continuation(
            &record,
            &first_overlay.projection,
            &first_overlay.owners,
            &[second],
        )?;
        assert_eq!(second_overlay.fills.len(), 1);
        assert!(second_overlay.projection.open_orders.is_empty());
        assert_eq!(
            position_quantity(&second_overlay.projection, PositionSide::Long)?,
            Decimal::from(12)
        );
        assert_eq!(
            position_quantity(&second_overlay.projection, PositionSide::Short)?,
            Decimal::from(8)
        );
        Ok(())
    }

    #[test]
    fn unsent_terminal_history_does_not_block_live_paired_fills()
    -> Result<(), Box<dyn std::error::Error>> {
        let working = vec![
            owner(
                "live-long",
                "native-long",
                PositionSide::Long,
                GridOrderRole::Open,
                1,
            )?,
            owner(
                "live-short",
                "native-short",
                PositionSide::Short,
                GridOrderRole::Close,
                2,
            )?,
        ];
        let baseline = projection(&working)?;
        let events = working
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                owner_event(
                    owner,
                    u64::try_from(index + 1)?,
                    owner.quantity,
                    owner.quantity,
                    OrderState::Filled,
                )
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        let mut owners = working;
        for index in 0..1248 {
            let mut historical = owner(
                &format!("unsent-{index}"),
                "unused",
                PositionSide::Long,
                GridOrderRole::Open,
                1,
            )?;
            historical.config_revision = 1;
            historical.plan_revision = 3;
            historical.state = GridOwnedOrderState::Terminal;
            historical.native_order_id = None;
            owners.push(historical);
        }
        let overlaid = apply_stream_overlay(&record()?, &baseline, &owners, &events)?;
        assert_eq!(overlaid.fills.len(), 2);
        assert!(overlaid.projection.open_orders.is_empty());
        assert_eq!(&overlaid.owners[2..], &owners[2..]);
        assert_eq!(
            position_quantity(&overlaid.projection, PositionSide::Long)?,
            Decimal::from(12)
        );
        assert_eq!(
            position_quantity(&overlaid.projection, PositionSide::Short)?,
            Decimal::from(8)
        );
        Ok(())
    }

    #[test]
    fn missing_native_identity_is_rejected_for_working_or_filled_owners()
    -> Result<(), Box<dyn std::error::Error>> {
        let working = owner(
            "live",
            "native-live",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?;
        let baseline = projection(std::slice::from_ref(&working))?;
        let fill = owner_event(
            &working,
            1,
            working.quantity,
            working.quantity,
            OrderState::Filled,
        )?;
        let mut invalid = working.clone();
        invalid.native_order_id = None;
        assert_eq!(
            apply_stream_overlay(
                &record()?,
                &baseline,
                &[invalid],
                std::slice::from_ref(&fill)
            ),
            Err(GridStreamOverlayError::Ownership)
        );
        let mut historical = owner(
            "invalid-history",
            "unused",
            PositionSide::Short,
            GridOrderRole::Open,
            2,
        )?;
        historical.native_order_id = None;
        historical.state = GridOwnedOrderState::Terminal;
        historical.filled_quantity = Decimal::ONE;
        assert_eq!(
            apply_stream_overlay(&record()?, &baseline, &[working, historical], &[fill]),
            Err(GridStreamOverlayError::Ownership)
        );
        Ok(())
    }

    #[test]
    fn prior_plan_working_owner_coexists_with_reused_historical_terminal_key()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut working = owner(
            "client-current-working",
            "native-current-working",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?;
        working.plan_revision = 6;

        let mut historical = owner(
            "client-historical-terminal",
            "native-historical-terminal",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?;
        historical.config_revision = 1;
        historical.plan_revision = 3;
        historical.state = GridOwnedOrderState::Terminal;

        let baseline = projection(std::slice::from_ref(&working))?;
        let fill = owner_event(
            &working,
            1,
            Decimal::from(2),
            Decimal::from(2),
            OrderState::Filled,
        )?;
        let overlaid = apply_stream_overlay(
            &record()?,
            &baseline,
            &[historical.clone(), working],
            &[fill],
        )?;

        assert!(overlaid.projection.open_orders.is_empty());
        assert_eq!(overlaid.owners[0], historical);
        assert!(
            overlaid
                .owners
                .iter()
                .all(|owner| owner.state == GridOwnedOrderState::Terminal)
        );
        assert_eq!(overlaid.fills.len(), 1);
        Ok(())
    }

    #[test]
    fn duplicate_working_semantic_keys_are_rejected_across_plan_revisions()
    -> Result<(), Box<dyn std::error::Error>> {
        let current = owner(
            "client-current",
            "native-current",
            PositionSide::Short,
            GridOrderRole::Close,
            1,
        )?;
        let mut survivor = owner(
            "client-survivor",
            "native-survivor",
            PositionSide::Short,
            GridOrderRole::Close,
            1,
        )?;
        survivor.plan_revision = 6;
        let owners = vec![current, survivor];
        let baseline = projection(&owners)?;
        let fill = owner_event(
            &owners[0],
            1,
            Decimal::from(2),
            Decimal::from(2),
            OrderState::Filled,
        )?;

        assert_eq!(
            apply_stream_overlay(&record()?, &baseline, &owners, &[fill]),
            Err(GridStreamOverlayError::Ownership)
        );
        Ok(())
    }

    #[test]
    fn historical_terminal_owner_cannot_match_an_idempotent_stream_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let working = owner(
            "client-working",
            "native-working",
            PositionSide::Short,
            GridOrderRole::Open,
            2,
        )?;
        let mut historical = owner(
            "client-historical",
            "native-historical",
            PositionSide::Long,
            GridOrderRole::Open,
            1,
        )?;
        historical.config_revision = 1;
        historical.plan_revision = 3;
        historical.filled_quantity = historical.quantity;
        historical.state = GridOwnedOrderState::Terminal;

        let mut baseline = projection(std::slice::from_ref(&working))?;
        let fill = owner_event(
            &historical,
            1,
            Decimal::from(2),
            Decimal::from(2),
            OrderState::Filled,
        )?;
        baseline
            .fills
            .push(terminal_fill(&fill, PositionSide::Long));

        assert_eq!(
            apply_stream_overlay(&record()?, &baseline, &[historical, working], &[fill]),
            Err(GridStreamOverlayError::Ownership)
        );
        Ok(())
    }
}
