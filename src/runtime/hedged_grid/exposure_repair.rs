use std::collections::BTreeMap;

use rust_decimal::Decimal;

use crate::strategy::hedged_grid::{
    GridInventory, GridOrderIntent, GridOrderKey, GridOrderRole, GridPhase, GridPosition,
    HedgedGridError, HedgedGridState,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ExposureLadderRepairPlan {
    AwaitingMakerReplay,
    Ready {
        target: BTreeMap<GridOrderKey, GridOrderIntent>,
        cancel: Vec<GridOrderKey>,
        place: Vec<GridOrderIntent>,
    },
}

/// Computes the smallest physical repair against the reducer's existing same-epoch ladder.
/// It never creates a new anchor or invents a replacement for an unprocessed maker fill.
pub(crate) fn plan_same_anchor_exposure_repair(
    state: &HedgedGridState,
    final_inventory: &GridInventory,
    signed_orders: &BTreeMap<GridOrderKey, GridOrderIntent>,
) -> Result<ExposureLadderRepairPlan, HedgedGridError> {
    final_inventory.validate()?;
    let Some(epoch) = state.epoch.as_ref() else {
        return Err(HedgedGridError::Epoch);
    };
    if state.phase != GridPhase::Running {
        return Ok(ExposureLadderRepairPlan::AwaitingMakerReplay);
    }
    if !state.pending_transactions.is_empty()
        || [GridPosition::Long, GridPosition::Short]
            .into_iter()
            .any(|position| {
                state
                    .owned_orders
                    .keys()
                    .filter(|key| {
                        key.epoch == epoch.epoch
                            && key.position == position
                            && key.role == GridOrderRole::Open
                    })
                    .count()
                    != usize::from(state.params.grid_count)
            })
    {
        return Ok(ExposureLadderRepairPlan::AwaitingMakerReplay);
    }

    let mut target = BTreeMap::new();
    for position in [GridPosition::Long, GridPosition::Short] {
        for (key, intent) in state.owned_orders.iter().filter(|(key, _)| {
            key.epoch == epoch.epoch && key.position == position && key.role == GridOrderRole::Open
        }) {
            target.insert(
                key.clone(),
                signed_orders
                    .get(key)
                    .filter(|signed| same_order_semantics(intent, signed))
                    .cloned()
                    .unwrap_or_else(|| intent.clone()),
            );
        }

        let available = match position {
            GridPosition::Long => final_inventory.long_quantity,
            GridPosition::Short => final_inventory.short_quantity,
        };
        let mut reserved = Decimal::ZERO;
        let mut closing_count = 0_u8;
        for (key, intent) in state.owned_orders.iter().filter(|(key, _)| {
            key.epoch == epoch.epoch && key.position == position && key.role == GridOrderRole::Close
        }) {
            if closing_count >= state.params.grid_count {
                continue;
            }
            let retained = signed_orders
                .get(key)
                .filter(|signed| same_order_semantics(intent, signed))
                .cloned()
                .unwrap_or_else(|| intent.clone());
            let Some(next_reserved) = reserved.checked_add(retained.quantity) else {
                return Err(HedgedGridError::Inventory);
            };
            if next_reserved > available {
                continue;
            }
            reserved = next_reserved;
            closing_count = closing_count.saturating_add(1);
            target.insert(key.clone(), retained);
        }
    }

    let cancel = signed_orders
        .keys()
        .filter(|key| !target.contains_key(*key))
        .cloned()
        .collect::<Vec<_>>();
    let place = target
        .iter()
        .filter(|(key, intent)| {
            signed_orders
                .get(*key)
                .is_none_or(|signed| signed != *intent)
        })
        .map(|(_, intent)| intent.clone())
        .collect::<Vec<_>>();

    Ok(ExposureLadderRepairPlan::Ready {
        target,
        cancel,
        place,
    })
}

fn same_order_semantics(expected: &GridOrderIntent, signed: &GridOrderIntent) -> bool {
    expected.key == signed.key
        && expected.side == signed.side
        && expected.price == signed.price
        && expected.reduce_only == signed.reduce_only
        && signed.quantity > Decimal::ZERO
        && if expected.reduce_only {
            signed.quantity <= expected.quantity
        } else {
            // Execution may raise opening quantity to the venue's minimum notional. The signed
            // WAL-bound physical quantity is authoritative and must not be "repaired" downward.
            signed.quantity >= expected.quantity
        }
}
