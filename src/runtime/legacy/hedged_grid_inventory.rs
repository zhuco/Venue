use crate::{
    domain::{Position, PositionSide, Price},
    strategy::hedged_grid::{GridInventory, HedgedGridState},
};

use super::{HedgedGridLiveError, PrivateFactsSnapshot};

pub(in crate::runtime) fn strategy_private_generation(
    state: &HedgedGridState,
    snapshot: &PrivateFactsSnapshot,
) -> Result<u64, HedgedGridLiveError> {
    if snapshot.generation == 0 || snapshot.observed_at_ms == 0 {
        return Err(HedgedGridLiveError::Snapshot);
    }
    let Some(previous) = state.inventory.as_ref() else {
        return Ok(snapshot.generation);
    };
    if snapshot.observed_at_ms < previous.private_observed_at_ms {
        return Err(HedgedGridLiveError::Snapshot);
    }
    if snapshot.observed_at_ms == previous.private_observed_at_ms {
        return Ok(previous.private_generation);
    }
    let next = previous
        .private_generation
        .checked_add(1)
        .ok_or(HedgedGridLiveError::Clock)?;
    Ok(next.max(snapshot.generation))
}

pub(in crate::runtime) fn inventory(
    snapshot: &PrivateFactsSnapshot,
    flat_public_mark: Price,
    private_generation: u64,
) -> Result<GridInventory, HedgedGridLiveError> {
    let long = position(&snapshot.positions, PositionSide::Long)?;
    let short = position(&snapshot.positions, PositionSide::Short)?;
    let mark_price = match (long.mark_price, short.mark_price) {
        (Some(long_mark), Some(short_mark)) if long_mark == short_mark => long_mark,
        (Some(mark), None) if short.quantity.is_zero() => mark,
        (None, Some(mark)) if long.quantity.is_zero() => mark,
        (None, None) if long.quantity.is_zero() && short.quantity.is_zero() => flat_public_mark,
        _ => return Err(HedgedGridLiveError::Inventory),
    };
    Ok(GridInventory {
        private_generation,
        private_observed_at_ms: snapshot.observed_at_ms,
        mark_price,
        long_quantity: long.quantity,
        short_quantity: short.quantity,
    })
}

fn position(positions: &[Position], side: PositionSide) -> Result<Position, HedgedGridLiveError> {
    let mut matching = positions.iter().filter(|position| position.side == side);
    let position = matching
        .next()
        .cloned()
        .ok_or(HedgedGridLiveError::Inventory)?;
    if matching.next().is_some() {
        return Err(HedgedGridLiveError::Inventory);
    }
    Ok(position)
}
