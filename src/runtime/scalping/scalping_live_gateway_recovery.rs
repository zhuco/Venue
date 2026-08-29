use super::{EXCHANGE, ScalpingLiveGatewayError};
use crate::{
    config::BinanceAccountBinding,
    domain::{NativeOrderFamily, OrderPurpose, is_canonical_trading_account_id},
    exchange::binance::{PrivateCredentials, PrivateError, PrivateRest},
    execution::{CommandJournal, Reconciler, resolve_unknown_order_by_readback},
    strategy::scalping::StrategyBinding,
};
use std::path::Path;

pub fn recover_unknown_scalping_cancels(
    artifacts_root: &Path,
    binding: &StrategyBinding,
    account_binding: BinanceAccountBinding,
    generation: u64,
    received_at_ms: u64,
) -> Result<bool, ScalpingLiveGatewayError> {
    let mut commands = CommandJournal::open(artifacts_root.join("commands.jsonl"))?;
    let command_ids = commands.unknown_protection_or_cancel_command_ids();
    if command_ids.is_empty() {
        return Ok(false);
    }
    let private =
        PrivateRest::production(PrivateCredentials::from_environment()?, account_binding)?;
    let mut facts = crate::storage::Journal::open(artifacts_root.join("recovery_facts.jsonl"))?;
    let mut reconciler =
        Reconciler::recover(&facts).map_err(|_| ScalpingLiveGatewayError::ReconciliationState)?;
    let mut resolved = false;
    for command_id in command_ids {
        let scoped = commands
            .cancel_target_identity(&command_id)
            .map(|identity| identity.owner)
            .or_else(|| {
                commands
                    .receipt(&command_id)
                    .and_then(|receipt| receipt.command.owner())
            })
            .is_some_and(|owner| {
                owner.strategy_instance_id == binding.strategy_instance_id
                    && owner.run_id == binding.run_id
                    && owner.exchange == binding.exchange
                    && owner.account == binding.account
                    && owner.symbol == binding.symbol
            });
        if scoped {
            let did_resolve = resolve_unknown_order_by_readback(
                &mut commands,
                &private,
                &mut facts,
                &mut reconciler,
                &command_id,
                generation,
                received_at_ms,
            )?;
            resolved |= did_resolve;
        }
    }
    Ok(resolved)
}

/// Resolves only an UNKNOWN scoped entry that three exact signed client-ID queries all prove was
/// never accepted. A current/historical order response, non-flat account, or any other error
/// remains fenced; this function never resubmits or cancels an order.
pub fn recover_absent_unknown_scalping_entry(
    artifacts_root: &Path,
    binding: &StrategyBinding,
    account_binding: BinanceAccountBinding,
) -> Result<bool, ScalpingLiveGatewayError> {
    if !artifacts_root.is_absolute()
        || binding.validate().is_err()
        || binding.exchange != EXCHANGE
        || !is_canonical_trading_account_id(&binding.account)
    {
        return Err(ScalpingLiveGatewayError::Settlement);
    }
    let mut commands = CommandJournal::open(artifacts_root.join("commands.jsonl"))?;
    let _ = commands.fence_interrupted_dispatches()?;
    let candidates = commands
        .recovery_identities()
        .into_iter()
        .filter(|(command_id, owner, family, _)| {
            *family == NativeOrderFamily::UmOrder
                && owner.strategy_instance_id == binding.strategy_instance_id
                && owner.run_id == binding.run_id
                && owner.exchange == binding.exchange
                && owner.account == binding.account
                && owner.symbol == binding.symbol
                && owner.purpose == OrderPurpose::Entry
                && matches!(
                    commands.receipt(command_id).map(|receipt| &receipt.state),
                    Some(crate::execution::CommandState::Unknown { .. })
                )
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(false);
    }
    if candidates.len() != 1 {
        return Err(ScalpingLiveGatewayError::UnresolvedCommand);
    }
    let (command_id, owner, _, client_id) = &candidates[0];
    let private =
        PrivateRest::production(PrivateCredentials::from_environment()?, account_binding)?;
    for _ in 0..3 {
        match private.order_by_client_id(&owner.symbol, client_id.as_str()) {
            Err(PrivateError::Rejected {
                api_code: Some(-2013),
                ..
            }) => {}
            Ok(_) => return Err(ScalpingLiveGatewayError::UnresolvedCommand),
            Err(error) => return Err(error.into()),
        }
    }
    let readback = private.readback(&binding.symbol)?;
    let flat = readback.positions.len() == 2
        && readback
            .positions
            .iter()
            .all(|position| position.quantity.is_zero())
        && readback.orders.is_empty();
    if !flat {
        return Err(ScalpingLiveGatewayError::UnresolvedCommand);
    }
    commands.transition(
        command_id,
        crate::execution::CommandState::Rejected {
            reason: "three_signed_client_id_queries_proved_order_absent".to_owned(),
        },
    )?;
    Ok(true)
}
