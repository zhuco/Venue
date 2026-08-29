use std::str::FromStr;

use crate::{
    runtime::account::{
        AccountKey, AccountModelError, ExchangeId, StrategyBinding, StrategyInstanceKey,
        StrategyKind,
    },
    strategy::hedged_grid::HedgedGridBinding,
};

/// Read-only identity bridge for the single-strategy Stage 7 runtime. It grants no writer, WAL,
/// transport or mutation capability and therefore cannot create a second live execution path.
pub fn legacy_stage7_strategy_binding(
    binding: &HedgedGridBinding,
) -> Result<StrategyBinding, AccountModelError> {
    let account = AccountKey::new(
        ExchangeId::from_str(&binding.exchange).map_err(|_| AccountModelError::Exchange)?,
        binding.account.clone(),
    )?;
    let key = StrategyInstanceKey::new(
        account,
        StrategyKind::HedgedGrid,
        binding.strategy_instance_id.clone(),
        binding.symbol.clone(),
    )?;
    StrategyBinding::new(key, binding.run_id.clone(), binding.config_version.clone())
}
