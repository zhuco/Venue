use rust_decimal::Decimal;

use crate::{
    domain::{
        CancelCommand, CommandId, FieldState, Order, OrderOwner, OrderPurpose, OrderSide,
        OrderState, PositionSide,
    },
    execution::{
        CommandJournal, CommandState, FlatReceipt, WriterLeaseAuthority, WriterScope,
        WriterSession, sha256_hex,
    },
    storage::PrivateEvidenceJournal,
    strategy::hedged_grid::HedgedGridBinding,
};

use super::{
    Stage7CanaryReport, Stage7CanaryVenue, Stage7GridError, canary_cleanup_readback,
    canary_preflight, command_id, reduce_canary_market, wait_for_canary_cleanup_position,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn fail_after_canary_error<V: Stage7CanaryVenue>(
    error: Stage7GridError,
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
    prefix: &str,
    suffix: u64,
) -> Result<Stage7CanaryReport, Stage7GridError> {
    recover_interrupted_canary(
        commands, venue, authority, writer, evidence, generation, binding, prefix, suffix,
    )
    .map_err(|_| Stage7GridError::CanaryCleanup)?;
    Err(error)
}

/// Uses only the existing writer, command journal and signed private facts to unwind a Canary
/// process that ended between an accepted mutation and its normal flat receipt.
#[allow(clippy::too_many_arguments)]
pub(super) fn recover_interrupted_canary<V: Stage7CanaryVenue>(
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
    prefix: &str,
    suffix: u64,
) -> Result<(), Stage7GridError> {
    cleanup_canary_exposure(
        commands, venue, authority, writer, evidence, generation, binding, prefix, suffix,
    )?;
    authority
        .retire_flat(&FlatReceipt {
            receipt_id: format!(
                "{}_stage7_failed_canary_flat_{}",
                binding.exchange, *generation
            ),
            predecessor: writer.clone(),
            scope: WriterScope {
                exchange: binding.exchange.clone(),
                account: binding.account.clone(),
                symbol: binding.symbol.clone(),
                owner_scope: binding.owner_scope.clone(),
            },
            readback_generation: *generation,
            summary_sha256: sha256_hex(format!(
                "{}_stage7_failed_canary_flat:{}:{}",
                binding.exchange, binding.owner_scope, *generation
            )),
        })
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
fn cleanup_canary_exposure<V: Stage7CanaryVenue>(
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
    prefix: &str,
    suffix: u64,
) -> Result<(), Stage7GridError> {
    let (readback, _) = canary_cleanup_readback(venue, evidence, generation, binding)?;
    let cancels = cleanup_cancels(commands, &readback.orders, binding, prefix, suffix)?;
    dispatch_cleanup_cancels(commands, venue, authority, writer, cancels)?;
    let (after_cancel, inventory) = canary_cleanup_readback(venue, evidence, generation, binding)?;
    if after_cancel
        .orders
        .iter()
        .any(|order| matches!(order.state, OrderState::New | OrderState::PartiallyFilled))
    {
        return Err(Stage7GridError::Canary);
    }
    flatten_leg(
        commands,
        venue,
        authority,
        writer,
        evidence,
        generation,
        binding,
        prefix,
        suffix,
        PositionSide::Long,
        inventory.long_quantity,
        inventory.mark_price,
    )?;
    flatten_leg(
        commands,
        venue,
        authority,
        writer,
        evidence,
        generation,
        binding,
        prefix,
        suffix,
        PositionSide::Short,
        inventory.short_quantity,
        inventory.mark_price,
    )?;
    let (final_readback, final_inventory) =
        canary_cleanup_readback(venue, evidence, generation, binding)?;
    canary_preflight(&final_readback, &final_inventory)
}

fn cleanup_cancels(
    commands: &CommandJournal,
    orders: &[Order],
    binding: &HedgedGridBinding,
    prefix: &str,
    suffix: u64,
) -> Result<Vec<CancelCommand>, Stage7GridError> {
    orders
        .iter()
        .filter(|order| matches!(order.state, OrderState::New | OrderState::PartiallyFilled))
        .enumerate()
        .map(|(index, order)| {
            let FieldState::Known(client_order_id) = &order.client_order_id else {
                return Err(Stage7GridError::ForeignOrders);
            };
            let client_order_id = CommandId::new(client_order_id.clone())
                .map_err(|_| Stage7GridError::ForeignOrders)?;
            let owner = commands
                .owner_by_client_id(&client_order_id)
                .filter(|owner| owner_matches_binding(owner, binding))
                .ok_or(Stage7GridError::ForeignOrders)?;
            Ok(CancelCommand {
                command_id: command_id(format!("{prefix}_{suffix}_cleanup_cancel_{index}"))?,
                owner: owner.clone(),
                target_client_order_id: client_order_id,
            })
        })
        .collect()
}

fn owner_matches_binding(owner: &OrderOwner, binding: &HedgedGridBinding) -> bool {
    owner.strategy_instance_id == binding.strategy_instance_id
        && owner.run_id == binding.run_id
        && owner.exchange == binding.exchange
        && owner.account == binding.account
        && owner.symbol == binding.symbol
}

fn cleanup_owner(binding: &HedgedGridBinding) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: binding.strategy_instance_id.clone(),
        run_id: binding.run_id.clone(),
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        purpose: OrderPurpose::Reduce,
    }
}

fn dispatch_cleanup_cancels<V: Stage7CanaryVenue>(
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    cancels: Vec<CancelCommand>,
) -> Result<(), Stage7GridError> {
    for cancel in cancels {
        commands.prepare_cancel(cancel.clone())?;
        commands.transition(&cancel.command_id, CommandState::Submitted)?;
        let _guard = authority.persistent_dispatch_guard(writer)?;
        match venue.mutation_client().cancel_by_client_id(&cancel) {
            Ok(venue_order_id) => {
                commands.transition(
                    &cancel.command_id,
                    CommandState::Accepted { venue_order_id },
                )?;
            }
            Err(error) => {
                // A cancel can race a venue-side fill or cancel.  A negative cancel response is
                // not enough to label the command resolved, but the exact client identity can
                // prove that its target already reached a terminal state.  This keeps cleanup
                // strict for an active target while avoiding a false UNKNOWN fence after a
                // terminal racing transition.
                match super::recover_cancel(cancel.target_client_order_id.as_str(), venue) {
                    Ok(Some(venue_order_id)) => {
                        commands.transition(
                            &cancel.command_id,
                            CommandState::Accepted { venue_order_id },
                        )?;
                    }
                    Ok(None) | Err(_) => {
                        commands.transition(
                            &cancel.command_id,
                            CommandState::Unknown {
                                reason: error.to_string(),
                            },
                        )?;
                        return Err(Stage7GridError::Unresolved);
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn flatten_leg<V: Stage7CanaryVenue>(
    commands: &mut CommandJournal,
    venue: &mut V,
    authority: &WriterLeaseAuthority,
    writer: &WriterSession,
    evidence: &mut PrivateEvidenceJournal,
    generation: &mut u64,
    binding: &HedgedGridBinding,
    prefix: &str,
    suffix: u64,
    position_side: PositionSide,
    quantity: Decimal,
    _mark_price: crate::domain::Price,
) -> Result<(), Stage7GridError> {
    if quantity.is_zero() {
        return Ok(());
    }
    let name = match position_side {
        PositionSide::Long => "lr",
        PositionSide::Short => "sr",
        PositionSide::Net => return Err(Stage7GridError::Canary),
    };
    let command = crate::domain::MarketReduceCommand {
        command_id: command_id(format!("{prefix}_{suffix}_cleanup_{name}_cmd"))?,
        client_order_id: command_id(format!("{prefix}_{suffix}_cleanup_{name}"))?,
        owner: OrderOwner {
            purpose: OrderPurpose::ExposureTakeProfit,
            ..cleanup_owner(binding)
        },
        side: match position_side {
            PositionSide::Long => OrderSide::Sell,
            PositionSide::Short => OrderSide::Buy,
            PositionSide::Net => return Err(Stage7GridError::Canary),
        },
        position_side,
        quantity,
        risk_episode_id: command_id(format!("{prefix}_{suffix}_cleanup_{name}_episode"))?,
        position_generation: *generation,
    };
    command.validate().map_err(|_| Stage7GridError::Canary)?;
    reduce_canary_market(commands, venue, authority, writer, command)?;
    let _ = wait_for_canary_cleanup_position(
        venue,
        evidence,
        generation,
        binding,
        position_side,
        false,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::domain::{OrderSide, Price, Symbol};

    use super::*;

    fn binding() -> Result<HedgedGridBinding, Box<dyn std::error::Error>> {
        Ok(HedgedGridBinding {
            strategy_instance_id: "hedged_grid_doge_usdt".to_owned(),
            run_id: "primary".to_owned(),
            exchange: "gate".to_owned(),
            account: "usdt_futures".to_owned(),
            symbol: "DOGE/USDT".parse::<Symbol>()?,
            config_version: "stage7".to_owned(),
            owner_scope: "hedged_grid_doge_usdt_primary".to_owned(),
        })
    }

    fn visible_foreign_order() -> Result<Order, Box<dyn std::error::Error>> {
        Ok(Order {
            time_in_force: venue_domain::FieldState::Known(Default::default()),
            order_id: "external-1".to_owned(),
            client_order_id: FieldState::Known("external-identity".to_owned()),
            symbol: "DOGE/USDT".parse()?,
            side: OrderSide::Buy,
            position_side: FieldState::Known(PositionSide::Long),
            purpose: FieldState::Known(OrderPurpose::Entry),
            state: OrderState::New,
            quantity: Decimal::ONE,
            filled_quantity: Decimal::ZERO,
            limit_price: Some(Price::new(Decimal::ONE)?),
            average_price: FieldState::Missing,
            reduce_only: false,
        })
    }

    #[test]
    fn cleanup_refuses_to_cancel_a_visible_foreign_order() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let journal = CommandJournal::open(temporary.path().join("commands.jsonl"))?;
        assert!(matches!(
            cleanup_cancels(&journal, &[visible_foreign_order()?], &binding()?, "gpc", 1),
            Err(Stage7GridError::ForeignOrders)
        ));
        Ok(())
    }
}
