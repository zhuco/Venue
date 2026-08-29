use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use crate::{
    domain::{FieldState, Order, OrderState, PositionSide},
    execution::CommandJournal,
    storage::ProjectionStore,
    strategy::hedged_grid::{
        GridOrderIntent, GridOrderKey, GridOrderRole, GridPhase, GridPosition,
    },
};

use super::{
    ORDER_HEALTH_FILE, Stage7GridCheckpoint, Stage7GridError, client_order_id,
    parse_grid_client_order_id,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Stage7GridHealthStatus {
    Healthy,
    Transitioning,
    Unhealthy,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Stage7GridHealthReport {
    pub schema_version: u16,
    pub exchange: String,
    pub account: String,
    pub symbol: String,
    pub checked_at_ms: u64,
    pub private_generation: u64,
    pub phase: GridPhase,
    pub status: Stage7GridHealthStatus,
    pub expected_orders: u16,
    pub observed_orders: u16,
    pub expected_long_opening: u8,
    pub observed_long_opening: u8,
    pub expected_short_opening: u8,
    pub observed_short_opening: u8,
    pub expected_long_closing: u8,
    pub observed_long_closing: u8,
    pub expected_short_closing: u8,
    pub observed_short_closing: u8,
    pub pending_transactions: u16,
    pub has_unresolved_wal: bool,
    pub problems: Vec<String>,
}

impl Stage7GridHealthReport {
    #[cfg(test)]
    pub(super) fn is_healthy(&self) -> bool {
        self.status == Stage7GridHealthStatus::Healthy
    }

    pub(super) fn is_unhealthy(&self) -> bool {
        self.status == Stage7GridHealthStatus::Unhealthy
    }
}

pub(super) fn transitioning_after_dispatch(
    mut report: Stage7GridHealthReport,
) -> Stage7GridHealthReport {
    // A full fill has just WAL-prepared a bounded replacement transaction. The readback at this
    // scheduled instant still lacks the filled predecessor, so it cannot be a stable grid yet.
    // Record that explicitly instead of silently postponing the 30-minute report. Any other
    // inconsistency, especially an unresolved WAL, remains unhealthy and closes mutation.
    if !report.has_unresolved_wal
        && report.problems == ["signed_open_orders_do_not_match_checkpoint"]
    {
        report.status = Stage7GridHealthStatus::Transitioning;
        report
            .problems
            .push("fill_replacement_pending_fresh_readback".to_owned());
    }
    report
}

pub(super) fn evaluate(
    checkpoint: &Stage7GridCheckpoint,
    commands: &CommandJournal,
    readback: &super::GridVenueReadback,
    private_generation: u64,
    checked_at_ms: u64,
) -> Stage7GridHealthReport {
    let expected = lane_counts(checkpoint.state.owned_orders.values());
    let mut observed = LaneCounts::default();
    let mut problems = Vec::new();
    let mut observed_keys = BTreeMap::new();

    for order in &readback.orders {
        let Some(key) = check_order(order, &checkpoint.state.owned_orders, &mut problems) else {
            continue;
        };
        if observed_keys
            .insert(key.clone(), order.order_id.clone())
            .is_some()
        {
            problems.push("duplicate_owned_order_key".to_owned());
            continue;
        }
        observed.add(&key);
    }

    let expected_orders = u16::try_from(checkpoint.state.owned_orders.len()).unwrap_or(u16::MAX);
    let observed_orders = u16::try_from(readback.orders.len()).unwrap_or(u16::MAX);
    if checkpoint.state.phase == GridPhase::Running {
        let grid_count = checkpoint.state.params.grid_count;
        if expected.long_opening != grid_count || expected.short_opening != grid_count {
            problems.push("expected_opening_grid_does_not_match_configured_count".to_owned());
        }
        if expected.long_closing > grid_count || expected.short_closing > grid_count {
            problems.push("expected_closing_grid_exceeds_configured_count".to_owned());
        }
        if expected != observed || expected_orders != observed_orders {
            problems.push("signed_open_orders_do_not_match_checkpoint".to_owned());
        }
    }
    let has_unresolved_wal = commands.has_unresolved();
    if has_unresolved_wal {
        problems.push("unresolved_wal".to_owned());
    }
    let status = if !problems.is_empty() || checkpoint.state.phase == GridPhase::BlockedUnknown {
        Stage7GridHealthStatus::Unhealthy
    } else {
        match checkpoint.state.phase {
            GridPhase::Running => Stage7GridHealthStatus::Healthy,
            GridPhase::Recovering
            | GridPhase::CheckingInventory
            | GridPhase::ResettingGrid
            | GridPhase::ReplenishingInventory
            | GridPhase::Stopping => Stage7GridHealthStatus::Transitioning,
            GridPhase::BlockedUnknown => Stage7GridHealthStatus::Unhealthy,
        }
    };
    Stage7GridHealthReport {
        schema_version: 1,
        exchange: checkpoint.binding.exchange.clone(),
        account: checkpoint.binding.account.clone(),
        symbol: checkpoint.binding.symbol.to_string(),
        checked_at_ms,
        private_generation,
        phase: checkpoint.state.phase,
        status,
        expected_orders,
        observed_orders,
        expected_long_opening: expected.long_opening,
        observed_long_opening: observed.long_opening,
        expected_short_opening: expected.short_opening,
        observed_short_opening: observed.short_opening,
        expected_long_closing: expected.long_closing,
        observed_long_closing: observed.long_closing,
        expected_short_closing: expected.short_closing,
        observed_short_closing: observed.short_closing,
        pending_transactions: u16::try_from(checkpoint.state.pending_transactions.len())
            .unwrap_or(u16::MAX),
        has_unresolved_wal,
        problems,
    }
}

pub(super) fn persist(
    artifacts_root: &Path,
    report: &Stage7GridHealthReport,
) -> Result<(), Stage7GridError> {
    ProjectionStore::new(artifacts_root.join(ORDER_HEALTH_FILE))
        .save(report)
        .map_err(Into::into)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct LaneCounts {
    long_opening: u8,
    short_opening: u8,
    long_closing: u8,
    short_closing: u8,
}

impl LaneCounts {
    fn add(&mut self, key: &GridOrderKey) {
        let target = match (key.position, key.role) {
            (GridPosition::Long, GridOrderRole::Open) => &mut self.long_opening,
            (GridPosition::Short, GridOrderRole::Open) => &mut self.short_opening,
            (GridPosition::Long, GridOrderRole::Close) => &mut self.long_closing,
            (GridPosition::Short, GridOrderRole::Close) => &mut self.short_closing,
        };
        *target = target.saturating_add(1);
    }
}

fn lane_counts<'a>(orders: impl Iterator<Item = &'a GridOrderIntent>) -> LaneCounts {
    let mut counts = LaneCounts::default();
    for order in orders {
        counts.add(&order.key);
    }
    counts
}

fn check_order(
    order: &Order,
    expected: &BTreeMap<GridOrderKey, GridOrderIntent>,
    problems: &mut Vec<String>,
) -> Option<GridOrderKey> {
    if !matches!(order.state, OrderState::New | OrderState::PartiallyFilled) {
        problems.push("open_order_has_terminal_state".to_owned());
        return None;
    }
    let FieldState::Known(actual_client_order_id) = &order.client_order_id else {
        problems.push("open_order_client_identity_is_not_known".to_owned());
        return None;
    };
    let key = match parse_grid_client_order_id(actual_client_order_id) {
        Ok(key) => key,
        Err(_) => {
            problems.push("open_order_client_identity_is_not_owned_grid_format".to_owned());
            return None;
        }
    };
    let Some(intent) = expected.get(&key) else {
        problems.push("open_order_is_not_in_checkpoint".to_owned());
        return None;
    };
    let expected_client_order_id = match client_order_id(&key) {
        Ok(value) => value,
        Err(_) => {
            problems.push("checkpoint_order_identity_is_invalid".to_owned());
            return None;
        }
    };
    if actual_client_order_id != expected_client_order_id.as_str() {
        problems.push("open_order_client_identity_is_not_exact".to_owned());
        return None;
    }
    let expected_position_side = match key.position {
        GridPosition::Long => PositionSide::Long,
        GridPosition::Short => PositionSide::Short,
    };
    if order.side != intent.side
        || order.position_side != FieldState::Known(expected_position_side)
        || order.reduce_only != intent.reduce_only
        || order.quantity != intent.quantity
        || order.limit_price != Some(intent.price)
    {
        problems.push("open_order_semantics_do_not_match_checkpoint".to_owned());
    }
    Some(key)
}
