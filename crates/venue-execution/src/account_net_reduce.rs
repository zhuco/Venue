use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use venue_domain::domain::{
    CommandId, ExecutionCommand, FieldState, MarketReduceCommand, PositionSide,
};
use venue_gateway_api::GatewayBinding;

use crate::{CommandJournal, CommandState};

use super::{
    AccountHostValidationError, COMMAND_JOURNAL_HARD_LIMIT_BYTES, SignedAccountPositionMode,
    SignedAccountSnapshot, snapshot_covers_binding_position_mode, valid_text,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct NetReduceSettlement {
    venue_order_id: String,
    #[serde(with = "rust_decimal::serde::str")]
    quantity: Decimal,
    position_generation: u64,
    settled_private_generation: u64,
    fill_ids: Vec<String>,
    #[serde(with = "rust_decimal::serde::str")]
    position_quantity: Decimal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) struct PersistedSignedBootstrap {
    pub(super) snapshot: SignedAccountSnapshot,
    pub(super) net_reduce_settlements: BTreeMap<CommandId, NetReduceSettlement>,
}

pub(super) fn completed_net_reduce_settlement(
    snapshot: &SignedAccountSnapshot,
    reduce: &MarketReduceCommand,
    venue_order_id: &str,
) -> Result<Option<NetReduceSettlement>, AccountHostValidationError> {
    if !valid_text(venue_order_id)
        || snapshot.position_mode() != SignedAccountPositionMode::Net
        || snapshot.private_generation() <= reduce.position_generation
    {
        return Ok(None);
    }
    let positions = snapshot
        .positions()
        .iter()
        .filter(|position| {
            position.symbol == reduce.owner.symbol && position.position_side == PositionSide::Net
        })
        .collect::<Vec<_>>();
    let [position] = positions.as_slice() else {
        return Ok(None);
    };
    if snapshot.open_orders().iter().any(|order| {
        order.client_order_id == reduce.client_order_id.as_str()
            || order.venue_order_id.as_deref() == Some(venue_order_id)
    }) {
        return Ok(None);
    }
    let mut fill_ids = BTreeSet::new();
    let mut total = Decimal::ZERO;
    for fill in snapshot
        .fills()
        .iter()
        .filter(|fill| fill.order_id == venue_order_id)
    {
        if fill.symbol != reduce.owner.symbol
            || fill.side != reduce.side
            || !matches!(fill.position_side, FieldState::Known(PositionSide::Net))
            || !fill_ids.insert(fill.fill_id.clone())
        {
            return Ok(None);
        }
        total = total
            .checked_add(fill.quantity)
            .ok_or(AccountHostValidationError::Notional)?;
    }
    if fill_ids.is_empty() || total != reduce.quantity {
        return Ok(None);
    }
    Ok(Some(NetReduceSettlement {
        venue_order_id: venue_order_id.to_owned(),
        quantity: reduce.quantity,
        position_generation: reduce.position_generation,
        settled_private_generation: snapshot.private_generation(),
        fill_ids: fill_ids.into_iter().collect(),
        position_quantity: position.quantity,
    }))
}

pub(super) fn validate_recovered_net_reduce_settlements(
    journal: &CommandJournal,
    snapshot: Option<&SignedAccountSnapshot>,
    settlements: &BTreeMap<CommandId, NetReduceSettlement>,
) -> Result<(), AccountHostValidationError> {
    if settlements.is_empty() {
        return Ok(());
    }
    let snapshot = snapshot.ok_or(AccountHostValidationError::SignedSnapshot)?;
    if snapshot.position_mode() != SignedAccountPositionMode::Net
        || !snapshot_covers_binding_position_mode(snapshot, snapshot.binding())
    {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    for (command_id, settlement) in settlements {
        let Some(receipt) = journal.receipt(command_id) else {
            return Err(AccountHostValidationError::Recovery);
        };
        let ExecutionCommand::MarketReduce(reduce) = &receipt.command else {
            return Err(AccountHostValidationError::Recovery);
        };
        let CommandState::Accepted { venue_order_id } = &receipt.state else {
            return Err(AccountHostValidationError::Recovery);
        };
        let valid = reduce.position_side == PositionSide::Net
            && venue_order_id == &settlement.venue_order_id
            && reduce.quantity == settlement.quantity
            && reduce.position_generation == settlement.position_generation
            && settlement.position_generation > 0
            && settlement.settled_private_generation > settlement.position_generation
            && snapshot.private_generation() >= settlement.settled_private_generation
            && settlement.quantity.is_sign_positive()
            && settlement.position_quantity != Decimal::MAX
            && settlement.position_quantity != Decimal::MIN
            && !settlement.fill_ids.is_empty()
            && settlement
                .fill_ids
                .iter()
                .all(|fill_id| valid_text(fill_id))
            && settlement.fill_ids.iter().collect::<BTreeSet<_>>().len()
                == settlement.fill_ids.len();
        if !valid {
            return Err(AccountHostValidationError::Recovery);
        }
    }
    Ok(())
}

pub(super) fn load_previous_signed_bootstrap(
    artifacts_root: &Path,
    binding: &GatewayBinding,
    bootstrap_file: &str,
) -> Result<Option<PersistedSignedBootstrap>, AccountHostValidationError> {
    let path = artifacts_root.join(bootstrap_file);
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(AccountHostValidationError::SignedSnapshot),
    };
    if !metadata.is_file() || metadata.len() > COMMAND_JOURNAL_HARD_LIMIT_BYTES {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    let encoded = fs::read(&path).map_err(|_| AccountHostValidationError::SignedSnapshot)?;
    let bootstrap = match serde_json::from_slice::<PersistedSignedBootstrap>(&encoded) {
        Ok(bootstrap) => bootstrap,
        Err(_) => PersistedSignedBootstrap {
            snapshot: serde_json::from_slice(&encoded)
                .map_err(|_| AccountHostValidationError::SignedSnapshot)?,
            net_reduce_settlements: BTreeMap::new(),
        },
    };
    if bootstrap.snapshot.binding() != binding
        || bootstrap.snapshot.fills_cursor().trim().is_empty()
    {
        return Err(AccountHostValidationError::SignedSnapshot);
    }
    Ok(Some(bootstrap))
}
