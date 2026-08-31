use rust_decimal::Decimal;

use crate::{
    domain::{CancelCommand, FieldState, OrderPurpose, OrderSide, OrderState, PositionSide, Price},
    execution::{CommandJournal, WriterLeaseAuthority, WriterSession},
    storage::PrivateEvidenceJournal,
    strategy::hedged_grid::HedgedGridBinding,
};

use super::{
    Stage7CanaryVenue, Stage7GridError, Stage7Mutation, assert_order_notional, canary_owner,
    canary_readback, command_id, execute_mutations, stage7_public_runtime::Stage7PublicRuntime,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn verify_reduce_only_post_only<V: Stage7CanaryVenue>(
    commands: &mut CommandJournal,
    venue: &mut V,
    public_market: &mut Stage7PublicRuntime,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
    prefix: &str,
    suffix: u64,
    position_side: PositionSide,
    quantity: Decimal,
    bid: Price,
    ask: Price,
) -> Result<(), Stage7GridError> {
    let (side, name, limit_price) = match position_side {
        PositionSide::Long => (
            OrderSide::Sell,
            "long",
            Price::new(ask.value() + venue.instrument().price_tick.value() * Decimal::from(10_u8))
                .map_err(|_| Stage7GridError::Notional)?,
        ),
        PositionSide::Short => (
            OrderSide::Buy,
            "short",
            Price::new(bid.value() - venue.instrument().price_tick.value() * Decimal::from(10_u8))
                .map_err(|_| Stage7GridError::Notional)?,
        ),
        PositionSide::Net => return Err(Stage7GridError::Canary),
    };
    let client_order_id = command_id(format!("{prefix}_{suffix}_{name}_lc"))?;
    let command = crate::domain::OrderCommand {
        time_in_force: Default::default(),
        command_id: command_id(format!("{prefix}_{suffix}_{name}_lc_cmd"))?,
        client_order_id: client_order_id.clone(),
        owner: canary_owner(binding, OrderPurpose::Reduce),
        side,
        position_side,
        quantity,
        limit_price,
        reduce_only: true,
    };
    command.validate().map_err(|_| Stage7GridError::Canary)?;
    assert_order_notional(command.quantity, command.limit_price, venue.instrument())?;
    execute_mutations(
        commands,
        venue,
        authority,
        writer,
        vec![Stage7Mutation::Place(command.clone())],
        true,
    )?;
    let (after_place, _, _, _) =
        canary_readback(venue, public_market, evidence, generation, binding)?;
    let visible = after_place.orders.iter().any(|order| {
        matches!(&order.client_order_id, FieldState::Known(value) if value == command.client_order_id.as_str())
            && order.side == command.side
            && order.position_side == FieldState::Known(position_side)
            && order.limit_price == Some(command.limit_price)
            && order.reduce_only
            && matches!(order.state, OrderState::New | OrderState::PartiallyFilled)
    });
    if !visible {
        return Err(Stage7GridError::Canary);
    }
    venue.verify_post_only_order(command.client_order_id.as_str())?;
    let cancel = CancelCommand {
        command_id: command_id(format!("{prefix}_{suffix}_{name}_lcx"))?,
        owner: canary_owner(binding, OrderPurpose::Reduce),
        target_client_order_id: client_order_id,
    };
    execute_mutations(
        commands,
        venue,
        authority,
        writer,
        vec![Stage7Mutation::Cancel(cancel)],
        true,
    )?;
    let (after_cancel, inventory, _, _) =
        canary_readback(venue, public_market, evidence, generation, binding)?;
    let leg_quantity = match position_side {
        PositionSide::Long => inventory.long_quantity,
        PositionSide::Short => inventory.short_quantity,
        PositionSide::Net => return Err(Stage7GridError::Canary),
    };
    if !after_cancel.orders.is_empty() || !leg_quantity.is_sign_positive() {
        return Err(Stage7GridError::Canary);
    }
    Ok(())
}
