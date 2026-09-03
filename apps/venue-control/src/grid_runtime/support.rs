use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};

use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use venue_control_protocol::{
    grid::{GridInstanceState, GridOrderRole as ProtocolOrderRole, GridOrderSemanticKey},
    kol::{ExecutorCommandState, TerminalAccountProjection, TerminalOpenOrder, TerminalPosition},
};
use venue_domain::domain::{OrderSide, PositionSide, Price};
use venue_gateway_binance::BinanceGridBootstrapMarketFacts;
use venue_strategies::hedged_grid::{
    GridBlockedReason, GridCloseReservations, GridInventory, GridOrderIntent, GridOrderRole,
    GridPlanner, GridPosition, GridResetTrigger, GridRollingAnchor,
};

use super::{
    ActualSurface, BinanceGridRuntimeError, GridDesiredOrder, GridDesiredSurface,
    GridOrderOwnership, GridOwnedOrderState, GridRuntimeRecord, RULE_VERSION_PREFIX,
    planner_anchor,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DesiredOrderMatch {
    Exact,
    Partial,
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MissingPlaceResult {
    Pending,
    FactsChanged,
    Failed(bool),
    ResetRequired,
}

pub(super) fn missing_place_result(
    status: Option<(ExecutorCommandState, u64)>,
    projection_observed_ms: u64,
    instance_updated_ms: u64,
) -> MissingPlaceResult {
    match status {
        Some((
            ExecutorCommandState::Pending
            | ExecutorCommandState::Sending
            | ExecutorCommandState::Accepted
            | ExecutorCommandState::ReconcileRequired,
            _,
        )) => MissingPlaceResult::Pending,
        Some((ExecutorCommandState::Reconciled, updated)) if projection_observed_ms <= updated => {
            MissingPlaceResult::Pending
        }
        Some((ExecutorCommandState::Rejected | ExecutorCommandState::Cancelled, updated)) => {
            MissingPlaceResult::Failed(updated >= instance_updated_ms)
        }
        Some((ExecutorCommandState::Reconciled, _)) => MissingPlaceResult::FactsChanged,
        None => MissingPlaceResult::ResetRequired,
    }
}

pub(super) fn prior_command_surfaces(
    desired: &GridDesiredSurface,
    ownership: &BTreeMap<String, GridOrderOwnership>,
    current_config_revision: u64,
    current_plan_revision: u64,
) -> BTreeSet<(u64, u64)> {
    desired
        .orders
        .iter()
        .filter_map(|order| ownership.get(&order.client_order_id))
        .map(|owner| (owner.config_revision, owner.plan_revision))
        .filter(|pair| *pair != (current_config_revision, current_plan_revision))
        .collect()
}

#[derive(Clone)]
pub(super) struct PrivateFacts {
    pub(super) inventory: GridInventory,
    pub(super) positions: Vec<TerminalPosition>,
}

pub(super) fn private_facts(
    record: &GridRuntimeRecord,
    projection: &TerminalAccountProjection,
    actual: &ActualSurface,
) -> Result<PrivateFacts, BinanceGridRuntimeError> {
    let positions = projection
        .positions
        .iter()
        .filter(|position| position.symbol == record.instance.symbol)
        .cloned()
        .collect::<Vec<_>>();
    let mut by_side = BTreeMap::new();
    for position in &positions {
        if position.quantity < Decimal::ZERO
            || by_side.insert(position.position_side, position).is_some()
        {
            return Err(BinanceGridRuntimeError::Facts);
        }
    }
    let long = by_side
        .get(&PositionSide::Long)
        .ok_or(BinanceGridRuntimeError::Facts)?;
    let short = by_side
        .get(&PositionSide::Short)
        .ok_or(BinanceGridRuntimeError::Facts)?;
    let marks = [long.mark_price, short.mark_price]
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    if marks.len() != 1 {
        return Err(BinanceGridRuntimeError::Facts);
    }
    let mark = *marks.first().ok_or(BinanceGridRuntimeError::Facts)?;
    let inventory = GridInventory {
        private_generation: projection.private_generation,
        private_observed_at_ms: projection.observed_ms,
        mark_price: Price::new(mark).map_err(|_| BinanceGridRuntimeError::Facts)?,
        long_quantity: long.quantity,
        short_quantity: short.quantity,
    };
    if actual.other_close_reservations.long_quantity > inventory.long_quantity
        || actual.other_close_reservations.short_quantity > inventory.short_quantity
    {
        return Err(BinanceGridRuntimeError::Facts);
    }
    Ok(PrivateFacts {
        inventory,
        positions,
    })
}

pub(super) fn fill_complete(
    total: Decimal,
    original: Decimal,
    still_open: bool,
) -> Result<bool, BinanceGridRuntimeError> {
    if total > original {
        return Err(BinanceGridRuntimeError::Facts);
    }
    Ok(!still_open && total == original)
}

pub(super) fn market_status(
    states: impl Iterator<Item = (ExecutorCommandState, u64)>,
    observed_ms: u64,
) -> (bool, bool, u64) {
    let (mut pending, mut failed, mut latest, mut latest_failure) = (false, false, 0, 0);
    for (state, updated) in states {
        latest = latest.max(updated);
        let rejected = matches!(
            state,
            ExecutorCommandState::Rejected | ExecutorCommandState::Cancelled
        );
        pending |= matches!(
            state,
            ExecutorCommandState::Pending
                | ExecutorCommandState::Sending
                | ExecutorCommandState::Accepted
                | ExecutorCommandState::ReconcileRequired
        );
        failed |= rejected;
        if rejected {
            latest_failure = latest_failure.max(updated);
        }
    }
    (!pending && observed_ms > latest, failed, latest_failure)
}

pub(super) fn is_nonterminal(state: ExecutorCommandState) -> bool {
    matches!(
        state,
        ExecutorCommandState::Pending
            | ExecutorCommandState::Sending
            | ExecutorCommandState::Accepted
            | ExecutorCommandState::ReconcileRequired
    )
}

pub(super) fn signed_teardown_ready(
    actual_empty: bool,
    unresolved_command: bool,
    latest_command_ms: Option<u64>,
    projection_observed_ms: u64,
) -> bool {
    actual_empty
        && !unresolved_command
        && latest_command_ms.is_none_or(|latest| projection_observed_ms > latest)
}

pub(super) fn desired_closes_fit(
    desired: &GridDesiredSurface,
    inventory: &GridInventory,
    reservations: &GridCloseReservations,
    actual: &BTreeMap<String, TerminalOpenOrder>,
    ownership: &BTreeMap<String, GridOrderOwnership>,
    completed_clients: &BTreeSet<String>,
) -> Result<bool, BinanceGridRuntimeError> {
    let (mut long, mut short) = (Decimal::ZERO, Decimal::ZERO);
    for order in desired
        .orders
        .iter()
        .filter(|order| order.key.role == ProtocolOrderRole::Close)
    {
        let total = match order.key.position_side {
            PositionSide::Long => &mut long,
            PositionSide::Short => &mut short,
            PositionSide::Net => return Err(BinanceGridRuntimeError::Facts),
        };
        let required = if let Some(actual) = actual.get(&order.client_order_id) {
            remaining_quantity(actual)?
        } else if completed_clients.contains(&order.client_order_id)
            || ownership
                .get(&order.client_order_id)
                .is_some_and(|owner| owner.state == GridOwnedOrderState::Terminal)
        {
            Decimal::ZERO
        } else {
            order.quantity
        };
        *total = total
            .checked_add(required)
            .ok_or(BinanceGridRuntimeError::Facts)?;
    }
    let long = long
        .checked_add(reservations.long_quantity)
        .ok_or(BinanceGridRuntimeError::Facts)?;
    let short = short
        .checked_add(reservations.short_quantity)
        .ok_or(BinanceGridRuntimeError::Facts)?;
    Ok(long <= inventory.long_quantity && short <= inventory.short_quantity)
}

pub(super) fn desired_orders(
    instance_id: &str,
    config_revision: u64,
    intents: &[GridOrderIntent],
    prior: Option<&GridDesiredSurface>,
    next_plan_revision: u64,
) -> Result<Vec<GridDesiredOrder>, BinanceGridRuntimeError> {
    let mut result = Vec::with_capacity(intents.len());
    let mut reused = BTreeSet::new();
    for intent in intents {
        let semantic = GridPlanner::semantic_order_key(intents, &intent.key)
            .map_err(|_| BinanceGridRuntimeError::Planner)?;
        let key = GridOrderSemanticKey {
            position_side: protocol_position(semantic.position),
            role: protocol_role(semantic.role),
            level: u16::from(semantic.grid_level),
            sequence: semantic.sequence,
        };
        let existing = prior.and_then(|surface| {
            surface.orders.iter().find(|order| {
                prior_order_reusable(order, &key, intent.quantity, intent.price.value(), &reused)
            })
        });
        let client_order_id = if let Some(order) = existing {
            reused.insert(order.client_order_id.clone());
            order.client_order_id.clone()
        } else {
            durable_id(
                "vgp",
                instance_id,
                config_revision,
                next_plan_revision,
                &key.encoded(),
                36,
            )
        };
        result.push(GridDesiredOrder {
            key,
            client_order_id,
            quantity: intent.quantity,
            limit_price: intent.price.value(),
        });
    }
    result.sort_by_key(order_priority);
    Ok(result)
}

pub(super) fn order_priority(order: &GridDesiredOrder) -> (u8, String) {
    (
        u8::from(order.key.role != ProtocolOrderRole::Close),
        order.key.encoded(),
    )
}

pub(super) fn durable_id(
    prefix: &str,
    instance: &str,
    config: u64,
    plan: u64,
    semantic: &str,
    max_len: usize,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(instance);
    hasher.update(config.to_be_bytes());
    hasher.update(plan.to_be_bytes());
    hasher.update(semantic);
    let hex = format!("{:x}", hasher.finalize());
    let take = max_len.saturating_sub(prefix.len() + 1).min(hex.len());
    format!("{prefix}-{}", &hex[..take])
}

pub(super) fn rule_version(generation: u64) -> String {
    format!("{RULE_VERSION_PREFIX}-r{generation}")
}

pub(super) fn protocol_position(value: GridPosition) -> PositionSide {
    match value {
        GridPosition::Long => PositionSide::Long,
        GridPosition::Short => PositionSide::Short,
    }
}

pub(super) fn strategy_position(
    value: PositionSide,
) -> Result<GridPosition, BinanceGridRuntimeError> {
    match value {
        PositionSide::Long => Ok(GridPosition::Long),
        PositionSide::Short => Ok(GridPosition::Short),
        PositionSide::Net => Err(BinanceGridRuntimeError::Facts),
    }
}

pub(super) fn protocol_role(value: GridOrderRole) -> ProtocolOrderRole {
    match value {
        GridOrderRole::Open => ProtocolOrderRole::Open,
        GridOrderRole::Close => ProtocolOrderRole::Close,
    }
}

pub(super) fn strategy_role(value: ProtocolOrderRole) -> GridOrderRole {
    match value {
        ProtocolOrderRole::Open => GridOrderRole::Open,
        ProtocolOrderRole::Close => GridOrderRole::Close,
    }
}

pub(super) fn side_name(value: PositionSide) -> &'static str {
    match value {
        PositionSide::Long => "long",
        PositionSide::Short => "short",
        PositionSide::Net => "net",
    }
}

pub(super) fn prior_order_reusable(
    prior: &GridDesiredOrder,
    next: &GridOrderSemanticKey,
    next_quantity: Decimal,
    next_price: Decimal,
    reused: &BTreeSet<String>,
) -> bool {
    prior.key.position_side == next.position_side
        && prior.key.role == next.role
        && prior.key.sequence == next.sequence
        && prior.limit_price == next_price
        && next_quantity > Decimal::ZERO
        && next_quantity <= prior.quantity
        && !reused.contains(&prior.client_order_id)
}

pub(super) fn remaining_quantity(
    order: &TerminalOpenOrder,
) -> Result<Decimal, BinanceGridRuntimeError> {
    let filled = order
        .filled_quantity
        .ok_or(BinanceGridRuntimeError::Facts)?;
    if filled < Decimal::ZERO || filled > order.quantity {
        return Err(BinanceGridRuntimeError::Facts);
    }
    order
        .quantity
        .checked_sub(filled)
        .ok_or(BinanceGridRuntimeError::Facts)
}

pub(super) fn is_close_order(order: &TerminalOpenOrder) -> bool {
    matches!(
        (order.position_side, order.order_side),
        (PositionSide::Long, OrderSide::Sell) | (PositionSide::Short, OrderSide::Buy)
    )
}

pub(super) fn actual_matches_desired(
    order: &TerminalOpenOrder,
    desired: &GridDesiredOrder,
) -> Result<DesiredOrderMatch, BinanceGridRuntimeError> {
    let remaining = remaining_quantity(order)?;
    if order.position_side != desired.key.position_side
        || order.order_side != desired.key.order_side()
        || order.limit_price != Some(desired.limit_price)
        || !order.post_only
        || remaining <= Decimal::ZERO
        || remaining > desired.quantity
    {
        return Ok(DesiredOrderMatch::Conflict);
    }
    Ok(if remaining == desired.quantity {
        DesiredOrderMatch::Exact
    } else {
        DesiredOrderMatch::Partial
    })
}

pub(super) fn desired_digest(anchor: &GridRollingAnchor, orders: &[GridDesiredOrder]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(anchor.revision.to_be_bytes());
    hasher.update(anchor.instrument_generation.to_be_bytes());
    hasher.update(anchor.anchor_price.value().to_string());
    hasher.update(anchor.step.value().to_string());
    hasher.update(anchor.grid_quantity.to_string());
    for order in orders {
        hasher.update(order.key.encoded());
        hasher.update(&order.client_order_id);
        hasher.update(order.quantity.to_string());
        hasher.update(order.limit_price.to_string());
    }
    hasher.finalize().into()
}

pub(super) fn desired_valid_for_market(
    record: &GridRuntimeRecord,
    desired: &GridDesiredSurface,
    market: &BinanceGridBootstrapMarketFacts,
) -> bool {
    let rules = &market.rules;
    let Some(anchor) = record.instance.anchor.as_ref() else {
        return false;
    };
    desired.instance_id == record.instance.instance_id
        && desired.symbol == record.instance.symbol
        && desired.config_revision == record.instance.config_revision
        && desired.plan_revision == record.instance.plan_revision
        && anchor.revision == desired.plan_revision
        && anchor.instrument_generation == rules.instrument.generation
        && planner_anchor(anchor, record.instance.config_revision).is_ok_and(|rolling| {
            desired_digest(&rolling, &desired.orders) == desired.desired_digest
        })
        && desired.orders.iter().all(|order| {
            order.quantity >= rules.minimum_quantity
                && order.quantity <= rules.maximum_quantity
                && order.quantity % rules.instrument.quantity_step == Decimal::ZERO
                && order.limit_price >= rules.minimum_price
                && order.limit_price <= rules.maximum_price
                && order.limit_price % rules.instrument.price_tick.value() == Decimal::ZERO
                && order
                    .quantity
                    .checked_mul(order.limit_price)
                    .is_some_and(|notional| notional >= rules.instrument.minimum_notional.value)
        })
}

pub(super) fn empty_digest() -> [u8; 32] {
    Sha256::digest(b"venue-grid-empty-v1").into()
}

pub(super) fn empty_surface(
    record: &GridRuntimeRecord,
    digest: [u8; 32],
    plan_revision: u64,
) -> GridDesiredSurface {
    GridDesiredSurface {
        instance_id: record.instance.instance_id.clone(),
        symbol: record.instance.symbol.clone(),
        config_revision: record.instance.config_revision,
        plan_revision,
        desired_digest: digest,
        orders: Vec::new(),
    }
}

pub(super) fn blocked_code(reason: GridBlockedReason) -> &'static str {
    match reason {
        GridBlockedReason::InvalidMarketFacts => "market_invalid",
        GridBlockedReason::StaleMarketFacts => "market_stale",
        GridBlockedReason::InvalidPrivateFacts => "private_invalid",
        GridBlockedReason::StalePrivateFacts => "private_stale",
        GridBlockedReason::MissingRiskFacts => "risk_missing",
        GridBlockedReason::InvalidRiskFacts => "risk_invalid",
        GridBlockedReason::ReductionBelowMinimum => "reduction_below_minimum",
    }
}

pub(super) fn lifecycle_timeout_code(
    state: GridInstanceState,
    started_ms: Option<u64>,
    timeout_ms: u64,
    now_ms: u64,
) -> Option<&'static str> {
    let expired = started_ms.is_some_and(|started| now_ms.saturating_sub(started) > timeout_ms);
    if !expired {
        return None;
    }
    match state {
        GridInstanceState::Paused => Some("pause_convergence_timeout"),
        GridInstanceState::StopPending => Some("stop_convergence_timeout"),
        GridInstanceState::ResetRequired => Some("reset_convergence_timeout"),
        _ => None,
    }
}

pub(super) fn reset_code(value: GridResetTrigger) -> &'static str {
    use GridResetTrigger::*;
    match value {
        Manual => "manual_reset",
        RevisionMismatch => "revision_mismatch",
        InstrumentGenerationChanged => "instrument_changed",
        InvalidOwnedOrder => "owned_order_invalid",
        DuplicateOwnedOrder => "owned_order_duplicate",
        IncompleteOwnedSurface => "surface_incomplete",
        CompletedFillStillOpen => "fill_order_conflict",
        ConflictingFillEvidence => "fill_conflict",
        RollingConflict => "rolling_conflict",
        PriceWouldCrossBook => "price_cross",
        ConvergenceTimedOut => "convergence_timeout",
        FailureThresholdReached => "failure_threshold",
    }
}

pub(super) fn now_ms() -> Result<u64, BinanceGridRuntimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BinanceGridRuntimeError::Clock)?
        .as_millis()
        .try_into()
        .map_err(|_| BinanceGridRuntimeError::Clock)
}
