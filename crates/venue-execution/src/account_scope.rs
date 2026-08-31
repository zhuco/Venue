use rust_decimal::Decimal;
use venue_domain::domain::{Asset, ExecutionCommand, PositionSide};
use venue_gateway_api::GatewayBinding;

use super::{
    AccountHostValidationError, AccountRiskEvidence, AccountSymbolSet, CommandJournal,
    CommandState, SignedAccountPositionMode, SignedAccountSnapshot,
};

pub(super) fn validate_command_scope(
    command: &ExecutionCommand,
    binding: &GatewayBinding,
    configured_symbols: &AccountSymbolSet,
) -> Result<(), AccountHostValidationError> {
    command
        .validate_persisted_shape()
        .map_err(|_| AccountHostValidationError::Command)?;
    let owner = command.mutation_owner();
    if owner.exchange != binding.venue.as_str()
        || owner.account != binding.trading_account_id
        || !configured_symbols.contains(&owner.symbol)
    {
        return Err(AccountHostValidationError::Scope);
    }
    match command {
        ExecutionCommand::PlaceLimit(limit) if !limit.reduce_only => {
            let notional = limit
                .quantity
                .checked_mul(limit.limit_price.value())
                .ok_or(AccountHostValidationError::Notional)?;
            if notional <= rust_decimal::Decimal::ZERO {
                return Err(AccountHostValidationError::Notional);
            }
        }
        ExecutionCommand::PlaceMarket(_) => {
            return Err(AccountHostValidationError::MarketEntryDisabled);
        }
        ExecutionCommand::PlaceLimit(_)
        | ExecutionCommand::MarketReduce(_)
        | ExecutionCommand::StopMarketCloseAll(_)
        | ExecutionCommand::StopMarketFullPosition(_)
        | ExecutionCommand::Cancel(_) => {}
    }
    Ok(())
}

pub(super) fn is_risk_increasing(command: &ExecutionCommand) -> bool {
    matches!(command, ExecutionCommand::PlaceLimit(order) if !order.reduce_only)
        || matches!(command, ExecutionCommand::PlaceMarket(_))
}

pub(super) fn has_open_entry_reservation(journal: &CommandJournal) -> bool {
    journal.commands().any(|command| {
        let ExecutionCommand::PlaceLimit(place) = command else {
            return false;
        };
        !place.reduce_only
            && !journal.has_accepted_cancel_for(&place.client_order_id)
            && journal.receipt(&place.command_id).is_some_and(|receipt| {
                matches!(
                    receipt.state,
                    CommandState::Accepted { .. } | CommandState::Unknown { .. }
                )
            })
    })
}

pub(super) fn wal_entry_reservation_total(
    journal: &CommandJournal,
    evidence: &AccountRiskEvidence,
) -> Result<Decimal, AccountHostValidationError> {
    journal
        .commands()
        .filter_map(|command| match command {
            ExecutionCommand::PlaceLimit(place)
                if !place.reduce_only
                    && !journal.has_accepted_cancel_for(&place.client_order_id) =>
            {
                Some(place)
            }
            _ => None,
        })
        .filter(|place| {
            journal.receipt(&place.command_id).is_some_and(|receipt| {
                matches!(
                    receipt.state,
                    CommandState::Submitted
                        | CommandState::Accepted { .. }
                        | CommandState::Unknown { .. }
                )
            })
        })
        .try_fold(Decimal::ZERO, |total, place| {
            let value = place
                .quantity
                .checked_mul(place.limit_price.value())
                .filter(|notional| *notional > Decimal::ZERO)
                .ok_or(AccountHostValidationError::Notional)?;
            let asset = Asset::new(place.owner.symbol.quote())
                .map_err(|_| AccountHostValidationError::RiskEvidence)?;
            let value = evidence.value_in_usdt(&asset, value)?;
            total
                .checked_add(value)
                .ok_or(AccountHostValidationError::Notional)
        })
}

pub(super) fn snapshot_covers_configured_symbols(
    snapshot: &SignedAccountSnapshot,
    configured_symbols: &AccountSymbolSet,
) -> bool {
    configured_symbols
        .iter()
        .all(|symbol| match snapshot.position_mode() {
            SignedAccountPositionMode::Net => {
                snapshot
                    .positions()
                    .iter()
                    .filter(|position| position.symbol == *symbol)
                    .all(|position| position.position_side == PositionSide::Net)
                    && snapshot
                        .positions()
                        .iter()
                        .filter(|position| position.symbol == *symbol)
                        .count()
                        <= 1
            }
            SignedAccountPositionMode::Hedge => {
                [PositionSide::Long, PositionSide::Short]
                    .iter()
                    .all(|side| {
                        snapshot
                            .positions()
                            .iter()
                            .filter(|position| {
                                position.symbol == *symbol && position.position_side == *side
                            })
                            .count()
                            == 1
                    })
                    && snapshot.positions().iter().all(|position| {
                        position.symbol != *symbol
                            || matches!(
                                position.position_side,
                                PositionSide::Long | PositionSide::Short
                            )
                    })
            }
        })
}

pub(super) fn snapshot_covers_binding_position_mode(
    snapshot: &SignedAccountSnapshot,
    binding: &GatewayBinding,
) -> bool {
    snapshot_covers_configured_symbols(snapshot, &AccountSymbolSet::single(binding))
}
