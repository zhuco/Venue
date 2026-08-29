use crate::{
    domain::{CommandId, OrderOwner, OrderPurpose},
    strategy::hedged_grid::{GridOrderKey, GridOrderRole, GridPosition, HedgedGridBinding},
};

use super::HedgedGridLiveError;

pub(in crate::runtime) fn client_order_id(
    key: &GridOrderKey,
) -> Result<CommandId, HedgedGridLiveError> {
    let role = match key.role {
        GridOrderRole::Open => "open",
        GridOrderRole::Close => "close",
    };
    CommandId::new(format!(
        "hgo_e{}_{}_{}_l{}",
        key.epoch,
        position_name(key.position),
        role,
        key.level
    ))
    .map_err(|_| HedgedGridLiveError::Identifier)
}

pub(super) fn position_name(position: GridPosition) -> &'static str {
    match position {
        GridPosition::Long => "long",
        GridPosition::Short => "short",
    }
}

pub(super) fn owner(binding: &HedgedGridBinding, purpose: OrderPurpose) -> OrderOwner {
    OrderOwner {
        strategy_instance_id: binding.strategy_instance_id.clone(),
        run_id: binding.run_id.clone(),
        exchange: binding.exchange.clone(),
        account: binding.account.clone(),
        symbol: binding.symbol.clone(),
        purpose,
    }
}
